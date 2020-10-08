package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type minioConfig struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"ak"`
	SecretAccessKey string `json:"sk"`
	Ssl             bool   `json:""ssl`
	Bucket          string `json:"bucket"`
	Path            string `json:"path"`
}

type minioP struct {
	minioConfig minioConfig
	minioClient *minio.Client
}

//Init init kafak client
func InitMinio() *minioP {
	log.Info("start connect mino....")
	content, err := ioutil.ReadFile("./plugins/minio/minio.json")
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	// log.Info(string(content))
	var config minioConfig
	config.Ssl = true
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}
	c := &minioP{minioConfig: config}
	c.connect()
	return c
}

//connect
func (m *minioP) connect() {
	ctx := context.Background()

	// Initialize minio client object.
	minioClient, err := minio.New(m.minioConfig.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(m.minioConfig.AccessKeyID, m.minioConfig.SecretAccessKey, ""),
		Secure: m.minioConfig.Ssl,
	})
	if err != nil {
		log.Fatal("Connect: ", zap.Error(err))
	}

	// Make a new bucket called mymusic.
	err = minioClient.MakeBucket(ctx, m.minioConfig.Bucket, minio.MakeBucketOptions{Region: m.minioConfig.Region})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, m.minioConfig.Bucket)
		if errBucketExists == nil && exists {
			log.Info("We already own ", zap.String("bucket", m.minioConfig.Bucket))
		} else {
			log.Fatal("Bucket exsit ", zap.Error(err))
		}
	} else {
		log.Info("Successfully created ", zap.String("bucket", m.minioConfig.Bucket))
	}

	m.minioClient = minioClient
}

//Save to minio
func (m *minioP) Publish(e *Elements) error {
	timeStamp := time.Now().UTC().UnixNano()
	ctx := context.Background()
	reader := strings.NewReader(e.Payload)
	objectName := fmt.Sprintf("%s/%d", e.ClientID, timeStamp)
	if m.minioConfig.Path != "" {
		objectName = fmt.Sprintf("%s/%s", m.minioConfig.Path, objectName)
	}
	log.Info("Put object", zap.String("name", objectName))

	info, err := m.minioClient.PutObject(ctx, m.minioConfig.Bucket, objectName, reader, int64(reader.Size()),
		minio.PutObjectOptions{ContentType: "text"})
	if err != nil {
		log.Fatal("Upload ", zap.Error(err))
		return err
	}

	log.Info("Successfully uploaded and size ", zap.String("buckect", info.Bucket), zap.Int64("size", info.Size))
	return nil
}
