// This source file is part of the EdgeDB open source project.
//
// Copyright 2020-present EdgeDB Inc. and the EdgeDB authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snc

import "sync"

// NewServerSettings returns an empty ServerSettings.
func NewServerSettings() *ServerSettings {
	return &ServerSettings{settings: make(map[string][]byte)}
}

// ServerSettings is a concurrency safe map. A ServerSettings must
// not be copied after first use. Use NewServerSettings() instead of creating
// ServerSettings manually.
type ServerSettings struct {
	settings map[string][]byte
	mx       sync.Mutex
}

// GetOk returns the value for key.
func (s *ServerSettings) GetOk(key string) ([]byte, bool) {
	val, ok := s.settings[key]
	return val, ok
}

// Get returns the value for key.
func (s *ServerSettings) Get(key string) []byte {
	return s.settings[key]
}

// Set sets the value for key.
func (s *ServerSettings) Set(key string, val []byte) {
	s.mx.Lock()
	defer s.mx.Unlock()
	newSettings := make(map[string][]byte, len(s.settings)+1)
	for k, v := range s.settings {
		newSettings[k] = v
	}
	newSettings[key] = val
	s.settings = newSettings
}
