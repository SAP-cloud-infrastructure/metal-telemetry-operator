// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package utils provides shared helper utilities for the telemetry operator.
package utils

import (
	"context"
	"crypto/rand"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// HMACKeyDataKey is the Secret data field that holds the key bytes.
	HMACKeyDataKey = "key"
	// HMACKeyLen is the required key length in bytes (256-bit).
	HMACKeyLen = 32
)

// EnsureHMACKey reads the named Secret from the given namespace. If it does
// not exist or contains an invalid key, a new 32-byte random key is generated
// and stored. The key bytes are returned. Concurrent calls (e.g. from multiple
// replicas starting simultaneously) are safe: if a concurrent write wins, this
// function reads and returns that key instead.
func EnsureHMACKey(ctx context.Context, c client.Client, namespace, name string) ([]byte, error) {
	secret := &corev1.Secret{}
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret)
	if err == nil {
		k := secret.Data[HMACKeyDataKey]
		if len(k) == HMACKeyLen {
			return k, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get HMAC key secret %s/%s: %w", namespace, name, err)
	}

	key := make([]byte, HMACKeyLen)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate HMAC key: %w", err)
	}

	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string][]byte{HMACKeyDataKey: key},
	}
	if err := c.Create(ctx, newSecret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create HMAC key secret %s/%s: %w", namespace, name, err)
		}
		// Another replica won the race — read the secret it created.
		if err2 := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err2 != nil {
			return nil, fmt.Errorf("get HMAC key secret after conflict %s/%s: %w", namespace, name, err2)
		}
		k := secret.Data[HMACKeyDataKey]
		if len(k) != HMACKeyLen {
			return nil, fmt.Errorf("HMAC key secret %s/%s has invalid key length %d", namespace, name, len(k))
		}
		return k, nil
	}
	return key, nil
}
