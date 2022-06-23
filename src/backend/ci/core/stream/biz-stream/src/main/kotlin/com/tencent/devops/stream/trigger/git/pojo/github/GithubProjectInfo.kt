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

package com.tencent.devops.stream.trigger.git.pojo.github

import com.tencent.devops.common.sdk.github.pojo.Repository
import com.tencent.devops.repository.pojo.git.GitProjectInfo
import com.tencent.devops.stream.trigger.git.pojo.StreamGitProjectInfo

data class GithubProjectInfo(
    override val gitProjectId: String,
    override val defaultBranch: String?,
    override val gitHttpUrl: String,
    override val name: String,
    override val gitSshUrl: String?,
    override val homepage: String?,
    override val gitHttpsUrl: String?,
    override val description: String?,
    override val avatarUrl: String?,
    override val pathWithNamespace: String?,
    override val nameWithNamespace: String
) : StreamGitProjectInfo {
    constructor(g: GitProjectInfo) : this(
        gitProjectId = g.id.toString(),
        defaultBranch = g.defaultBranch,
        gitHttpUrl = g.repositoryUrl,
        name = g.name,
        gitSshUrl = g.gitSshUrl,
        homepage = g.homepage,
        gitHttpsUrl = g.gitHttpsUrl,
        description = g.description,
        avatarUrl = g.avatarUrl,
        pathWithNamespace = g.pathWithNamespace,
        nameWithNamespace = g.namespaceName
    )

    constructor(g: Repository) : this(
        gitProjectId = g.gitProjectId.toString(),
        defaultBranch = g.defaultBranch,
        gitHttpUrl = g.gitHttpUrl,
        name = g.name,
        gitSshUrl = g.gitSshUrl,
        homepage = g.homepage,
        gitHttpsUrl = g.gitHttpUrl,
        description = g.description,
        avatarUrl = g.avatarUrl,
        pathWithNamespace = g.nameWithNamespace,
        nameWithNamespace = g.nameWithNamespace
    )
}
