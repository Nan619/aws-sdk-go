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
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/DATA-DOG/godog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ObjectFeatureContext provides feature context for object.
func ObjectFeatureContext(s *godog.Suite) {
	s.Step(`^put object with key "(.{1,})"$`, putObjectWithKey)
	s.Step(`^put object status code is (\d+)$`, putObjectStatusCodeIs)

	s.Step(`^get object with key "(.{1,})"$`, getObjectWithKey)
	s.Step(`^get object status code is (\d+)$`, getObjectStatusCodeIs)
	s.Step(`^get object content length is (\d+)$`, getObjectContentLengthIs)

	s.Step(`^delete object with key "(.{1,})"$`, deleteObjectWithKey)
	s.Step(`^delete object status code is (\d+)$`, deleteObjectStatusCodeIs)
}

// --------------------------------------------------------------------------
var putObjectOutputs []*s3.PutObjectOutput

func putObjectWithKey(objectKey string) error {
	_, err = exec.Command("dd", "if=/dev/zero", "of=/tmp/sdk_bin", "bs=1024", "count=1").Output()
	if err != nil {
		return err
	}
	defer os.Remove("/tmp/sdk_bin")

	errChan := make(chan error, tc.Concurrency)
	putObjectOutputs = make([]*s3.PutObjectOutput, tc.Concurrency)

	wg := sync.WaitGroup{}
	wg.Add(tc.Concurrency)
	for i := 0; i < tc.Concurrency; i++ {
		go func(index int, errChan chan<- error) {
			wg.Done()

			file, err := os.Open("/tmp/sdk_bin")
			if err != nil {
				errChan <- err
				return
			}
			defer file.Close()

			hash := md5.New()
			_, err = io.Copy(hash, file)
			if err != nil {
				errChan <- err
				return
			}
			hashInBytes := hash.Sum(nil)[:16]
			md5String := hex.EncodeToString(hashInBytes)

			//file.Seek(0, io.SeekStart)
			file.Seek(0, 0)
			if len(objectKey) > 1000 {
				objectKey = objectKey[:1000]
			}

			key := fmt.Sprintf("%s-%d", objectKey, index)
			putObjectOutput, err := svc.PutObject(&s3.PutObjectInput{
				ContentType: aws.String("text/plain"),
				ContentMD5:  aws.String(md5String),
				Body:        file,
				Bucket:      &bucket,
				Key:         &key,
			})

			if err != nil {
				errChan <- err
				return
			}
			putObjectOutputs[index] = putObjectOutput
			errChan <- nil
			return
		}(i, errChan)
	}
	wg.Wait()

	for i := 0; i < tc.Concurrency; i++ {
		err = <-errChan
		if err != nil {
			return err
		}
	}
	return nil
}

func putObjectStatusCodeIs(statusCode int) error {
	return nil
}

// --------------------------------------------------------------------------

var getObjectOutputs []*s3.GetObjectOutput

func getObjectWithKey(objectKey string) error {
	errChan := make(chan error, tc.Concurrency)
	getObjectOutputs = make([]*s3.GetObjectOutput, tc.Concurrency)

	wg := sync.WaitGroup{}
	wg.Add(tc.Concurrency)
	for i := 0; i < tc.Concurrency; i++ {
		go func(index int, errChan chan<- error) {
			wg.Done()

			if len(objectKey) > 1000 {
				objectKey = objectKey[:1000]
			}
			key := fmt.Sprintf("%s-%d", objectKey, index)
			getObjectOutput, err := svc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    &key,
			})
			if err != nil {
				errChan <- err
				return
			}
			getObjectOutputs[index] = getObjectOutput
			errChan <- nil
			return
		}(i, errChan)
	}
	wg.Wait()

	for i := 0; i < tc.Concurrency; i++ {
		err = <-errChan
		if err != nil {
			return err
		}
	}
	return nil
}

func getObjectStatusCodeIs(statusCode int) error {
	return nil
}

func getObjectContentLengthIs(length int) error {
	buffer := &bytes.Buffer{}
	for _, output := range getObjectOutputs {
		buffer.Truncate(0)
		buffer.ReadFrom(output.Body)
		err = checkEqual(len(buffer.Bytes())*1024, length)
		if err != nil {
			return err
		}
	}
	return nil
}

// --------------------------------------------------------------------------

var deleteObjectOutputs []*s3.DeleteObjectOutput

func deleteObjectWithKey(objectKey string) error {
	errChan := make(chan error, tc.Concurrency)
	deleteObjectOutputs = make([]*s3.DeleteObjectOutput, tc.Concurrency)

	wg := sync.WaitGroup{}
	wg.Add(tc.Concurrency)
	for i := 0; i < tc.Concurrency; i++ {
		go func(index int, errChan chan<- error) {
			wg.Done()

			if len(objectKey) > 1000 {
				objectKey = objectKey[:1000]
			}

			key := fmt.Sprintf("%s-%d", objectKey, index)
			deleteObjectOutput, err := svc.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key)})
			if err != nil {
				errChan <- err
				return
			}

			err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				errChan <- err
				return
			}

			deleteObjectOutputs[index] = deleteObjectOutput
			errChan <- nil
			return
		}(i, errChan)
	}
	wg.Wait()

	for i := 0; i < tc.Concurrency; i++ {
		err = <-errChan
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteObjectStatusCodeIs(statusCode int) error {
	return nil
}
