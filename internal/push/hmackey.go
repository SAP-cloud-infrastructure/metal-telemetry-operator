// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"context"
	"crypto/rand"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	hmacKeyDataKey = "key"
	hmacKeyLen     = 32
)

// ensureHMACKey returns the 32-byte HMAC key stored in the named Secret,
// creating a new random key if the Secret does not exist yet. Concurrent
// creations are handled gracefully via AlreadyExists retry.
func ensureHMACKey(ctx context.Context, c client.Client, namespace, name string) ([]byte, error) {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: namespace, Name: name}
	if err := c.Get(ctx, key, secret); err == nil {
		if k, ok := secret.Data[hmacKeyDataKey]; ok && len(k) == hmacKeyLen {
			return k, nil
		}
		return nil, fmt.Errorf("hmac key Secret %s/%s exists but has no valid %q entry", namespace, name, hmacKeyDataKey)
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get hmac key Secret %s/%s: %w", namespace, name, err)
	}

	raw := make([]byte, hmacKeyLen)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("failed to generate hmac key: %w", err)
	}

	immutable := true
	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Immutable: &immutable,
		Data:      map[string][]byte{hmacKeyDataKey: raw},
	}
	if err := c.Create(ctx, newSecret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err2 := c.Get(ctx, key, secret); err2 != nil {
				return nil, fmt.Errorf("hmac key Secret created concurrently but unreadable: %w", err2)
			}
			if k, ok := secret.Data[hmacKeyDataKey]; ok && len(k) == hmacKeyLen {
				return k, nil
			}
			return nil, fmt.Errorf("concurrently created hmac key Secret has no valid %q entry", hmacKeyDataKey)
		}
		return nil, fmt.Errorf("failed to create hmac key Secret %s/%s: %w", namespace, name, err)
	}
	return raw, nil
}
