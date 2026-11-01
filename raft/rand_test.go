package raft_test

import "math/rand"

func newRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }
