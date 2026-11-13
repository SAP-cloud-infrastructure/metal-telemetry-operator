// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import "time"

// SetTestPending seeds a testEntry into the reconciler's pending map.
// Used by external tests to simulate a sent test event without calling
// the real Redfish endpoint.
func (r *SubscriptionReconciler) SetTestPending(bmcName, messageId string, timeoutSecs int) {
	r.testPending.Store(bmcName, testEntry{
		messageId: messageId,
		deadline:  time.Now().Add(time.Duration(timeoutSecs) * time.Second),
	})
}
