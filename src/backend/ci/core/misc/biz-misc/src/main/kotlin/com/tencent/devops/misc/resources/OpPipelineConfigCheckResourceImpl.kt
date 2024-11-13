package com.tencent.devops.misc.resources

import com.tencent.devops.common.api.pojo.Result
import com.tencent.devops.common.api.util.JsonUtil
import com.tencent.devops.common.client.Client
import com.tencent.devops.common.pipeline.Model
import com.tencent.devops.common.redis.RedisLock
import com.tencent.devops.common.redis.RedisOperation
import com.tencent.devops.common.web.RestResource
import com.tencent.devops.misc.api.OpPipelineConfigCheckResource
import com.tencent.devops.misc.config.MiscBuildDataClearConfig
import com.tencent.devops.misc.cron.process.PipelineBuildHistoryDataClearJob
import com.tencent.devops.misc.dao.process.ProcessDao
import com.tencent.devops.notify.api.service.ServiceNotifyResource
import com.tencent.devops.plugin.codecc.CodeccApi
import org.apache.commons.collections4.MapUtils
import org.jooq.DSLContext
import org.jooq.Record7
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import java.util.*
import java.util.concurrent.*


@RestResource
class OpPipelineConfigCheckResourceImpl @Autowired constructor(
    private val miscBuildDataClearConfig: MiscBuildDataClearConfig,
    private val processDao: ProcessDao,
    private val dslContext: DSLContext,
    private val codeccApi: CodeccApi,
    private val redisOperation: RedisOperation,
    private val client: Client
): OpPipelineConfigCheckResource  {

    private var lock: RedisLock = RedisLock(redisOperation, LOCK_WRITE_KEY, 60 * 60 * 24 * 7)
    private var scanLock: RedisLock = RedisLock(redisOperation, LOCK_SCAN_KEY, 60 * 60 * 24 * 7)
    companion object {
        private val logger = LoggerFactory.getLogger(PipelineBuildHistoryDataClearJob::class.java)
        //流水线扫描最大的id key
        private const val PIPELINE_SCAN_MAX_ID_KEY = "pipeline:scan:max:id"
        private const val SCAN_COMPLETED_KEY = "scan:completed"
        private const val LOCK_WRITE_KEY = "pipelineMaxIdWrite"
        private const val LOCK_SCAN_KEY = "pipelineScan"
        private const val PIPELINE_SCAN_RESULT_KEY="pipeline:scan:result"

        private var executor: ThreadPoolExecutor? = null
        private var pipelineScanMap: Map<String, String>? = null

    }

    override fun pipelineConfigCheck(): Result<String> {


        val lockAcquired =scanLock.tryLock()
        if (!lockAcquired) {
            logger.error("Failed to acquire lock for pipeline scan")
        }
        pipelineScanMap =redisOperation.hentries(PIPELINE_SCAN_RESULT_KEY)
        if (executor == null) {
            executor = ThreadPoolExecutor(
                miscBuildDataClearConfig.maxThreadHandleProjectNum,
                miscBuildDataClearConfig.maxThreadHandleProjectNum,
                0L,
                TimeUnit.MILLISECONDS,
                LinkedBlockingQueue(10),
                Executors.defaultThreadFactory(),
                ThreadPoolExecutor.DiscardPolicy()
            )
        }

        // 获取上次处理的流水线 ID
        val lastProcessedId = redisOperation.get(PIPELINE_SCAN_MAX_ID_KEY)?.toLong() ?: 0L
        val maxPipelineId = processDao.getMaxId(dslContext)
        val maxThreadHandleProjectNum = miscBuildDataClearConfig.maxThreadHandleProjectNum
        val avgProjectNum = maxPipelineId / maxThreadHandleProjectNum

        // 检查是否需要重置扫描状态
        val scanCompletedValue = redisOperation.get(SCAN_COMPLETED_KEY)
        var isRescanNeeded = true
        if (scanCompletedValue == null) {
            //表示是第一次调用 默认进行全表扫描
            redisOperation.set(PIPELINE_SCAN_MAX_ID_KEY, "0")
            redisOperation.set(SCAN_COMPLETED_KEY, "false")
        } else {
            isRescanNeeded = scanCompletedValue == "true"
            if (isRescanNeeded) {
                // 重置标记，开始全量扫描
                redisOperation.set(PIPELINE_SCAN_MAX_ID_KEY, "0")
                redisOperation.set(SCAN_COMPLETED_KEY, "false")
            }
        }
        val futures = mutableListOf<Future<Boolean>>()
        for (index in 1..maxThreadHandleProjectNum) {
            // 计算线程能处理的最大项目主键 ID
            val maxThreadProjectPrimaryId = if (index != maxThreadHandleProjectNum) {
                index * avgProjectNum
            } else {
                index * avgProjectNum + maxPipelineId % maxThreadHandleProjectNum
            }

            val minPipelineId = if (isRescanNeeded) {
                (index - 1) * avgProjectNum
            } else {
                // 确保 lastProcessedId 在有效范围内
                when {
                    lastProcessedId >= (index - 1) * avgProjectNum && lastProcessedId <= maxThreadProjectPrimaryId -> {
                        lastProcessedId
                    }

                    lastProcessedId > maxThreadProjectPrimaryId -> {
                        // 如果 lastProcessedId 超过了当前线程的最大处理 ID，跳过当前迭代
                        continue
                    }

                    else -> {
                        (index - 1) * avgProjectNum
                    }
                }
            }
            val future = doCheckBus(
                minPipelineId = minPipelineId,
                maxPipelineId = maxThreadProjectPrimaryId
            )

            futures.add(future)
        }


        for (future in futures) {
            future.get()
        }
        // 在所有线程完成后，更新扫描完成标记
        redisOperation.set("SCAN_COMPLETED", "true")

        return Result("")
    }

    private fun doCheckBus(minPipelineId: Long, maxPipelineId: Long): Future<Boolean> {
        return executor!!.submit(Callable {
            try {
                val pipelineInfoWithSettings =
                    processDao.getAllPipelineInfoWithSettings(
                        dslContext,
                        minId = minPipelineId,
                        maxId = maxPipelineId
                    )

                pipelineInfoWithSettings.forEach { pipelineInfo ->
                    processPipelineInfo(pipelineInfo)
                }
            } catch (e: Exception) {
               logger.info("Error occurred while processing pipeline info: ${e.message}")
            }
            true
        })
    }
    private fun processPipelineInfo(pipelineInfo: Record7<String, Date, String, Int, String, String, Long>) {
        try {
            val tPipelineResource = TPipelineResource.T_PIPELINE_RESOURCE
            val tPipelineInfo = TPipelineInfo.T_PIPELINE_INFO
            val id = pipelineInfo[tPipelineInfo.ID] as Long
            val modelJson = pipelineInfo[tPipelineResource.MODEL] as String
            val model = JsonUtil.to(modelJson, Model::class.java);
            if (MapUtils.isNotEmpty(pipelineScanMap)) {
                val result = pipelineScanMap!![id.toString()]
                val elementIds = getElementIds(model)
                if (result != null && result.contains(elementIds)) {
                    return
                }
            }
            // 调用 codeccapi 接口进行扫描
            //  /prod/v2/apigw-app/codecc/scan/contentScan 换成 /open/v2/apigw-app/codecc/scan/contentScan
            codeccApi.getCodeccPipelineConfigResult(modelJson, "")
            //判断是否包含敏感信息 若包含  就发送通知
            // 发送邮箱扫描结果
            // client.get(ServiceNotifyResource::class).sendEmailNotify(generateEmailContent())
            //发送企业微信扫描结果
            //  client.get(ServiceNotifyResource::class).sendWeworkTextNotify(generateWeChatContent())
            val idsString = getElementIds(model)
            redisOperation.hmset(
                PIPELINE_SCAN_RESULT_KEY, mutableMapOf<String, String>(
                    "id" to id.toString(),
                    "elementId" to idsString
                )
            )
            //保证原子性 避免多线程写入覆盖
            lock.lock()
            val maxPipelineIdInRedis = redisOperation.get(PIPELINE_SCAN_MAX_ID_KEY)?.toLong()
            if (maxPipelineIdInRedis == null || id > maxPipelineIdInRedis) {
                redisOperation.set(PIPELINE_SCAN_MAX_ID_KEY, id.toString())
            }
        } catch (e: Exception) {
            logger.info("Error occurred while processing pipeline info: ${e.message}")
        } finally {
            lock.close()
        }

    }


    private fun getElementIds(model: Model): String {
        return model.stages.flatMap { stage ->
            stage.containers.flatMap { container ->
                container.elements.map { element ->
                    element.id
                }
            }
        }.joinToString(",")
    }



    private fun generateWeChatContent(
        sensitiveInfoCount: Int,
        pipelineName: String,
        lastModifier: String,
        lastModifiedTime: String,
        sensitiveInfoList: List<SensitiveInfo>
    ): String {
        val sensitiveInfoDetails = sensitiveInfoList.joinToString("\n") { info ->
            """
        --- 敏感信息 ---
        步骤：${info.step} - ${info.stepName}
        字段：${info.fieldCn} - ${info.fieldEn}
        敏感内容：${info.sensitiveContent}
        """.trimIndent()
        }

        return """
        【蓝盾流水线】敏感信息提醒 - 检测到 $sensitiveInfoCount 条敏感信息
        
        请检查流水线，谨防敏感信息泄漏：
        流水线：$pipelineName
        最近修改人/时间：$lastModifier $lastModifiedTime
        
        $sensitiveInfoDetails
        
        ---------------------
        [去处理]()
    """.trimIndent()
    }


    private fun generateEmailContent(
        pipelineName: String,
        version: String,
        lastModifier: String,
        lastModifiedTime: String,
        projectName: String,
        sensitiveInfoList: List<SensitiveInfo>
    ): String {
        val sensitiveInfoTable = sensitiveInfoList.joinToString("\n") { info ->
            "| ${info.step} | ${info.stepName} | ${info.fieldCn} | ${info.fieldEn} | ${info.sensitiveContent} |"
        }

        return """
            尊敬的蓝盾流水线拥有者，你好，

            检测到你负责的流水线编排中明文配置了敏感信息（如密码、Access token、Secret id、Secret key 等），请合理编写流水线，避免产生敏感信息泄露问题。
            流水线：$pipelineName; 版本号：$version; 最近修改人：$lastModifier; 最近修改时间：$lastModifiedTime; 所属项目：$projectName [去处理]()
            | 步骤 | 步骤名称 | 字段（中文名） | 字段（英文名） | 敏感内容 |
            | ------ | ------ | ------ | ------ | ------ |
            $sensitiveInfoTable

            敏感信息不可以直接写入到流水线配置里，流水线成员、项目管理员、上层SaaS系统均有可能查看配置、日志等，容易造成敏感信息扩散。
            推荐使用 [蓝盾凭据管理](https://iwiki.woa.com/p/1614662031) 功能加密管理凭据，在流水线中通过凭据 ID 引用凭据，流水线执行时实时获取凭据，敏感信息不落地。
            若插件入参未支持设置凭据 ID，直接接收敏感信息原文，可以使用 settings 上下文引用凭据或联系插件作者支持使用凭据 ID的方式。

            避免骚扰，此提醒邮件仅推送一次，请及时检查并处理。
            若有误报，也请反馈给我们完善检测逻辑，感谢支持。
        """.trimIndent()
    }

    data class SensitiveInfo(
        val step: String,
        val stepName: String,
        val fieldCn: String,
        val fieldEn: String,
        val sensitiveContent: String
    )
}