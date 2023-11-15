package com.tencent.devops.notify.model

import com.tencent.devops.notify.pojo.VoiceMessage

class VoiceNotifyMessageWithOperation : VoiceMessage() {
    var id: String? = null
    var retryCount: Int = 0
    var lastError: String? = null

    override fun toString(): String {
        return String.format(
            "id(%s), retryCount(%s), message(%s) ",
            id, retryCount, super.toString()
        )
    }
}
