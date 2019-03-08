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
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/DATA-DOG/godog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ObjectMultipartFeatureContext provides feature context for object multipart.
func ObjectMultipartFeatureContext(s *godog.Suite) {
	s.Step(`^initiate multipart upload with key "(.{1,})"$`, initiateMultipartUploadWithKey)
	s.Step(`^initiate multipart upload status code is (\d+)$`, initiateMultipartUploadStatusCodeIs)

	s.Step(`^upload the first part with key "(.{1,})"$`, uploadTheFirstPartWithKey)
	s.Step(`^upload the first part status code is (\d+)$`, uploadTheFirstPartStatusCodeIs)
	s.Step(`^upload the second part with key "(.{1,})"$`, uploadTheSecondPartWithKey)
	s.Step(`^upload the second part status code is (\d+)$`, uploadTheSecondPartStatusCodeIs)
	s.Step(`^upload the third part with key "(.{1,})"$`, uploadTheThirdPartWithKey)
	s.Step(`^upload the third part status code is (\d+)$`, uploadTheThirdPartStatusCodeIs)

	s.Step(`^list multipart with key "(.{1,})"$`, listMultipartWithKey)
	s.Step(`^list multipart status code is (\d+)$`, listMultipartStatusCodeIs)
	s.Step(`^list multipart object parts count is (\d+)$`, listMultipartObjectPartsCountIs)

	s.Step(`^complete multipart upload with key "(.{1,})"$`, completeMultipartUploadWithKey)
	s.Step(`^complete multipart upload status code is (\d+)$`, completeMultipartUploadStatusCodeIs)

	s.Step(`^abort multipart upload with key "(.{1,})"$`, abortMultipartUploadWithKey)
	s.Step(`^abort multipart upload status code is (\d+)$`, abortMultipartUploadStatusCodeIs)

	s.Step(`^delete the multipart object with key "(.{1,})"$`, deleteTheMultipartObjectWithKey)
	s.Step(`^delete the multipart object status code is (\d+)$`, deleteTheMultipartObjectStatusCodeIs)
}

// --------------------------------------------------------------------------

var createMultipartUploadOutput *s3.CreateMultipartUploadOutput

func initiateMultipartUploadWithKey(objectKey string) error {
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(objectKey),
		ContentType: aws.String("text/plain"),
	}
	createMultipartUploadOutput, err = svc.CreateMultipartUpload(input)
	return err
}

func initiateMultipartUploadStatusCodeIs(statusCode int) error {
	return nil
}

// --------------------------------------------------------------------------

var uploadTheFirstPartOutput *s3.UploadPartOutput
var uploadTheSecondPartOutput *s3.UploadPartOutput
var uploadTheThirdPartOutput *s3.UploadPartOutput

func uploadTheFirstPartWithKey(objectKey string) error {
	_, err = exec.Command("dd", "if=/dev/zero", "of=/tmp/sdk_bin_part_0", "bs=1048576", "count=5").Output()
	if err != nil {
		return err
	}
	defer os.Remove("/tmp/sdk_bin_part_0")

	file, err := os.Open("/tmp/sdk_bin_part_0")
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	partSize := fileInfo.Size()
	buffer := make([]byte, partSize)
	_, err = file.ReadAt(buffer, 0)
	if err != nil {
		return err
	}
	content := ioutil.NopCloser(bytes.NewReader(buffer))

	partInput := &s3.UploadPartInput{
		Body:       aws.ReadSeekCloser(content),
		Bucket:     createMultipartUploadOutput.Bucket,
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(int64(0)),
		UploadId:   createMultipartUploadOutput.UploadId,
	}
	uploadTheFirstPartOutput, err = svc.UploadPart(partInput)
	return err
}

func uploadTheFirstPartStatusCodeIs(statusCode int) error {
	return nil
}

func uploadTheSecondPartWithKey(objectKey string) error {
	_, err = exec.Command("dd", "if=/dev/zero", "of=/tmp/sdk_bin_part_1", "bs=1048576", "count=4").Output()
	if err != nil {
		return err
	}
	defer os.Remove("/tmp/sdk_bin_part_1")

	file, err := os.Open("/tmp/sdk_bin_part_1")
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	partSize := fileInfo.Size()
	buffer := make([]byte, partSize)
	_, err = file.ReadAt(buffer, 0)
	if err != nil {
		return err
	}
	content := ioutil.NopCloser(bytes.NewReader(buffer))

	partInput := &s3.UploadPartInput{
		Body:       aws.ReadSeekCloser(content),
		Bucket:     createMultipartUploadOutput.Bucket,
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(int64(1)),
		UploadId:   createMultipartUploadOutput.UploadId,
	}
	uploadTheSecondPartOutput, err = svc.UploadPart(partInput)
	return err
}

func uploadTheSecondPartStatusCodeIs(statusCode int) error {
	return nil
}

func uploadTheThirdPartWithKey(objectKey string) error {
	_, err = exec.Command("dd", "if=/dev/zero", "of=/tmp/sdk_bin_part_2", "bs=1048576", "count=3").Output()
	if err != nil {
		return err
	}
	defer os.Remove("/tmp/sdk_bin_part_2")

	file, err := os.Open("/tmp/sdk_bin_part_2")
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	partSize := fileInfo.Size()
	buffer := make([]byte, partSize)
	_, err = file.ReadAt(buffer, 0)
	if err != nil {
		return err
	}
	content := ioutil.NopCloser(bytes.NewReader(buffer))

	partInput := &s3.UploadPartInput{
		Body:       aws.ReadSeekCloser(content),
		Bucket:     createMultipartUploadOutput.Bucket,
		Key:        aws.String(objectKey),
		PartNumber: aws.Int64(int64(2)),
		UploadId:   createMultipartUploadOutput.UploadId,
	}
	uploadTheThirdPartOutput, err = svc.UploadPart(partInput)
	return err
}

func uploadTheThirdPartStatusCodeIs(statusCode int) error {
	return nil
}

// --------------------------------------------------------------------------

var listPartsOutput *s3.ListPartsOutput

func listMultipartWithKey(objectKey string) error {

	input := &s3.ListPartsInput{
		Bucket:   createMultipartUploadOutput.Bucket,
		Key:      aws.String(objectKey),
		UploadId: createMultipartUploadOutput.UploadId,
	}
	listPartsOutput, err = svc.ListParts(input)
	return err
}

func listMultipartStatusCodeIs(statusCode int) error {
	return nil
}

func listMultipartObjectPartsCountIs(count int) error {
	return checkEqual(len(listPartsOutput.Parts), count)
}

// --------------------------------------------------------------------------

var completeMultipartUploadOutput *s3.CompleteMultipartUploadOutput

func completeMultipartUploadWithKey(objectKey string) error {

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   createMultipartUploadOutput.Bucket,
		Key:      aws.String(objectKey),
		UploadId: createMultipartUploadOutput.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: []*s3.CompletedPart{
				{
					ETag:       uploadTheFirstPartOutput.ETag,
					PartNumber: aws.Int64(0),
				},
				{
					ETag:       uploadTheSecondPartOutput.ETag,
					PartNumber: aws.Int64(1),
				},
				{
					ETag:       uploadTheThirdPartOutput.ETag,
					PartNumber: aws.Int64(2),
				},
			},
		},
	}
	completeMultipartUploadOutput, err = svc.CompleteMultipartUpload(completeInput)
	return err
}

func completeMultipartUploadStatusCodeIs(statusCode int) error {
	return nil
}

// --------------------------------------------------------------------------

func abortMultipartUploadWithKey(objectKey string) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   createMultipartUploadOutput.Bucket,
		Key:      aws.String(objectKey),
		UploadId: createMultipartUploadOutput.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}

func abortMultipartUploadStatusCodeIs(statusCode int) error {
	return nil
}

// --------------------------------------------------------------------------

var deleteTheMultipartObjectOutput *s3.DeleteObjectOutput

func deleteTheMultipartObjectWithKey(objectKey string) error {

	deleteTheMultipartObjectOutput, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey)})
	if err != nil {
		return err
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	return err
}

func deleteTheMultipartObjectStatusCodeIs(statusCode int) error {
	return nil
}
