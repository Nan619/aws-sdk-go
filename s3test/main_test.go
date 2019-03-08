// +-------------------------------------------------------------------------
// | Copyright (C) 2016 Yunify, Inc.
// +-------------------------------------------------------------------------
// | Licensed under the Apache License, Version 2.0 (the "License");
// | you may not use this work except in compliance with the License.
// | You may obtain a copy of the License in the LICENSE file, or at:
// |
// | http://www.apache.org/licenses/LICENSE-2.0
// |
// | Unless required by applicable law or agreed to in writing, software
// | distributed under the License is distributed on an "AS IS" BASIS,
// | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// | See the License for the specific language governing permissions and
// | limitations under the License.
// +-------------------------------------------------------------------------

package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"testing"

	"github.com/DATA-DOG/godog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestMain(m *testing.M) {
	setUp()

	context := func(s *godog.Suite) {
		ObjectFeatureContext(s)
	}
	options := godog.Options{
		Format: "pretty",
		Paths:  []string{"./features"},
		Tags:   "",
	}
	status := godog.RunWithOptions("*", context, options)

	//tearDown()

	os.Exit(status)
}

var err error
var tc *testConfig
var bucket string
var svc *s3.S3

type testConfig struct {
	BucketName string `yaml:"bucket_name"`

	Region     string `yaml:"region"`
	Endpoint   string `yaml:"endpoint"`
	DisableSSL bool   `yaml:"disable_ssl"`

	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`

	Concurrency int `yaml:"concurrency"`
}

func loadTestConfig() {
	if tc == nil {
		configYAML, err := ioutil.ReadFile("./test_config.yaml")
		checkErrorForExit(err)

		tc = &testConfig{}
		err = yaml.Unmarshal(configYAML, tc)
		checkErrorForExit(err)
	}
}

func setUp() {
	loadTestConfig()

	bucket = tc.BucketName

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(tc.AccessKeyID, tc.SecretAccessKey, ""),
		Endpoint:    aws.String(tc.Endpoint),
		DisableSSL:  aws.Bool(tc.DisableSSL),
		Region:      aws.String(tc.Region)},
	)

	// Create S3 service client
	svc = s3.New(sess)

	_, err = svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	checkErrorForExit(err)

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &bucket})
	checkErrorForExit(err)
}

func tearDown() {
}
