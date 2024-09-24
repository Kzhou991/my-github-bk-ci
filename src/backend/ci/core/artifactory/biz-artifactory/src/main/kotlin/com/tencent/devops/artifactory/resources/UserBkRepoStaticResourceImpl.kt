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

package com.tencent.devops.artifactory.resources

import com.tencent.devops.artifactory.api.user.UserBkRepoStaticResource
import com.tencent.devops.artifactory.constant.BKREPO_STATIC_PROJECT_ID
import com.tencent.devops.artifactory.pojo.enums.FileChannelTypeEnum
import com.tencent.devops.artifactory.pojo.enums.FileTypeEnum
import com.tencent.devops.artifactory.service.ArchiveFileService
import com.tencent.devops.common.api.pojo.Result
import com.tencent.devops.common.api.util.DateTimeUtil
import com.tencent.devops.common.api.util.UUIDUtil
import com.tencent.devops.common.web.RestResource
import org.glassfish.jersey.media.multipart.FormDataContentDisposition
import org.springframework.beans.factory.annotation.Autowired
import java.io.InputStream
import java.time.LocalDateTime

@RestResource
class UserBkRepoStaticResourceImpl @Autowired constructor(
    private val archiveFileService: ArchiveFileService
) : UserBkRepoStaticResource {

    override fun uploadStaticFile(
        userId: String,
        inputStream: InputStream,
        disposition: FormDataContentDisposition,
        type: String?
    ): Result<String?> {
        val fileName = disposition.fileName
        val index = fileName.lastIndexOf(".")
        val fileSuffix = fileName.substring(index + 1)
        val filePathSb = StringBuilder("file/")
        val nowTime = DateTimeUtil.toDateTime(LocalDateTime.now(), DateTimeUtil.YYYYMMDD)
        val baseUrl = "$nowTime/${UUIDUtil.generate()}.$fileSuffix"
        val filePath = if (type.isNullOrBlank()) {
            filePathSb.append(baseUrl)
        } else {
            filePathSb.append("${type.lowercase()}/").append(baseUrl)
        }
        val url = archiveFileService.uploadFile(
            userId = userId,
            inputStream = inputStream,
            disposition = disposition,
            projectId = BKREPO_STATIC_PROJECT_ID,
            filePath = filePath.toString(),
            fileType = FileTypeEnum.BK_STATIC,
            fileChannelType = FileChannelTypeEnum.WEB_SHOW
        )
        return Result(url)
    }
}
