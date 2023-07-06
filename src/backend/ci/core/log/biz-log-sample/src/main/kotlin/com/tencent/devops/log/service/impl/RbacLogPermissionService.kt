package com.tencent.devops.log.service.impl

import com.tencent.devops.auth.api.service.ServicePermissionAuthResource
import com.tencent.devops.common.auth.api.AuthPermission
import com.tencent.devops.common.auth.api.AuthResourceType
import com.tencent.devops.common.auth.utils.RbacAuthUtils
import com.tencent.devops.common.client.Client
import com.tencent.devops.common.client.ClientTokenService
import com.tencent.devops.log.service.LogPermissionService
import org.springframework.beans.factory.annotation.Autowired

class RbacLogPermissionService @Autowired constructor(
    val client: Client,
    private val tokenCheckService: ClientTokenService
) : LogPermissionService {
    override fun verifyUserLogPermission(
        projectCode: String,
        pipelineId: String,
        userId: String,
        permission: AuthPermission?
    ): Boolean {
        /*val pipelineInfo =
            client.get(ServicePipelineResource::class).getPipelineInfo(
                projectId = projectCode,
                pipelineId = pipelineId,
                channelCode = null
            ).data ?: throw ErrorCodeException(
                errorCode = LogMessageCode.ERROR_PIPELINE_NOT_EXISTS
            )*/
        // 兼容CodeCC场景，CodeCC创建的流水线未向权限中心注册，调鉴权接口会报错。
        //pipelineInfo.channelCode != ChannelCode.BS ||
        return client.get(ServicePermissionAuthResource::class).validateUserResourcePermissionByRelation(
            userId = userId,
            token = tokenCheckService.getSystemToken(null) ?: "",
            action = RbacAuthUtils.buildAction(
                permission ?: AuthPermission.VIEW, AuthResourceType.PIPELINE_DEFAULT
            ),
            projectCode = projectCode,
            resourceType = AuthResourceType.PIPELINE_DEFAULT.value,
            resourceCode = pipelineId,
            relationResourceType = null
        ).data ?: false
    }
}
