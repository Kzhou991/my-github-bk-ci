/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.devops.auth.service

import com.tencent.devops.common.api.pojo.Result
import com.tencent.devops.common.auth.api.pojo.BKAuthProjectRolesResources
import com.tencent.devops.common.auth.api.pojo.BkAuthGroup
import com.tencent.devops.common.auth.api.pojo.BkAuthGroupAndUserList
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.MediaType

@Api(tags = ["AUTH_SERVICE_PROJECT"], description = "权限校验--项目相关")
@Path("/service/auth/project")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
interface ServiceProjectResource {

    @GET
    @Path("/projectCodes/{projectCode}/users/byGroup")
    @ApiOperation("获取项目成员 (需要对接的权限中心支持该功能才可以)")
    fun getProjectUsers(
        @QueryParam("serviceCode")
        @ApiParam("服务code", required = true)
        serviceCode: String,
        @PathParam("projectCode")
        @ApiParam("项目Code", required = true)
        projectCode: String,
        @QueryParam("group")
        @ApiParam("用户组类型", required = false)
        group: BkAuthGroup? = null
    ): Result<List<String>>

    @GET
    @Path("/projectCodes/{projectCode}/users")
    @ApiOperation("拉取项目所有成员，并按项目角色组分组成员信息返回")
    fun getProjectGroupAndUserList(
        @QueryParam("serviceCode")
        @ApiParam("服务code", required = true)
        serviceCode: String,
        @PathParam("projectCode")
        @ApiParam("项目Code", required = true)
        projectCode: String
    ): Result<List<BkAuthGroupAndUserList>>

    @GET
    @Path("/users/{userId}")
    @ApiOperation("获取用户有管理权限的项目Code")
    fun getUserProjects(
        @PathParam("userId")
        @ApiParam("用户userId", required = true)
        userId: String
    ): Result<String>

    @GET
    @Path("/users/{userId}/viewsAndManager")
    @ApiOperation("获取用户有查看或管理权限的项目")
    fun getUserProjectViewsAndManager(
        @PathParam("userId")
        @ApiParam("用户Id", required = true)
        userId: String
    ): Result<Map<String, String>>

    @GET
    @Path("projects/{projectCode}/users/{userId}/isProjectUsers")
    @ApiOperation("判断是否某个项目中某个组角色的成员")
    fun isProjectUser(
        @PathParam("userId")
        @ApiParam("用户Id", required = true)
        userId: String,
        @PathParam("projectCode")
        @ApiParam("项目Code", required = true)
        projectCode: String,
        @QueryParam("group")
        @ApiParam("用户组类型", required = false)
        group: BkAuthGroup? = null
    ): Result<Boolean>

    @POST
    @Path("projects/{projectCode}/createUser")
    @ApiOperation("添加用户到指定项目指定分组")
    fun createProjectUser(
        @QueryParam("userId")
        @ApiParam("用户Id", required = true)
        userId: String,
        @PathParam("projectCode")
        @ApiParam("项目Code", required = true)
        projectCode: String,
        @QueryParam("roleId")
        @ApiParam("用户组Id", required = true)
        role: String
    ): Result<Boolean>

    @GET
    @Path("/projects/{projectCode}/roles")
    @ApiOperation("获取项目角色")
    fun getProjectRoles(
        @PathParam("projectCode")
        @ApiParam("项目Code", required = true)
        projectCode: String,
        @QueryParam("projectId")
        @ApiParam("项目Id", required = true)
        projectId: String
    ): Result<List<BKAuthProjectRolesResources>>
}
