package com.tencent.devops.misc.api

import com.tencent.devops.common.api.auth.AUTH_HEADER_USER_ID
import com.tencent.devops.common.api.pojo.Result
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.tags.Tag
import javax.ws.rs.*
import javax.ws.rs.core.MediaType


@Tag(name = "OP_PROCESS_DB_MIGRATE", description = "op后台流水线配置信息检查")
@Path("/op/pipelineConfig/check")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
interface OpPipelineConfigCheckResource {


    @Operation(summary = "op后台流水线配置信息检查")
    @GET
    fun pipelineConfigCheck(): Result<String>
}

