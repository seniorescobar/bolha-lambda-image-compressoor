package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	log "github.com/sirupsen/logrus"
)

const (
	apiHost        = "api.tinify.com"
	s3ImagesBucket = "bolha-images"
)

var (
	s3d *s3manager.Downloader
	s3u *s3manager.Uploader
)

func Handler(ctx context.Context, event events.S3Event) error {
	sess := session.Must(session.NewSession())

	s3d = s3manager.NewDownloader(sess)
	s3u = s3manager.NewUploader(sess)

	tac := NewTinyAPIClient(os.Getenv("TINYAPIKEY"))

	for _, record := range event.Records {
		s3Obj := record.S3.Object

		imgKey := s3Obj.Key

		log.WithFields(log.Fields{
			"imgKey": imgKey,
		}).Info("downloading image")

		img, err := downloadS3Image(imgKey)
		if err != nil {
			return err
		}

		loc, err := tac.CompressImage(img)
		if err != nil {
			return err
		}

		imgNew, err := tac.ResizeCompressedImage(loc)
		if err != nil {
			return err
		}
		defer imgNew.Close()

		if err := uploadS3Image(strings.Replace(imgKey, "_uncompressed.jpg", ".jpg", 1), imgNew); err != nil {
			return err
		}
	}

	return nil
}

// HELPERS

func downloadS3Image(imgKey string) (io.Reader, error) {
	log.WithField("imgKey", imgKey).Info("downloading image from s3")

	buff := new(aws.WriteAtBuffer)

	_, err := s3d.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s3ImagesBucket),
		Key:    aws.String(imgKey),
	})
	if err != nil {
		return nil, err
	}

	imgBytes := buff.Bytes()

	return bytes.NewReader(imgBytes), nil
}

func uploadS3Image(imgKey string, img io.Reader) error {
	log.WithField("imgKey", imgKey).Info("uploading image to s3")

	if _, err := s3u.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3ImagesBucket),
		Key:    aws.String(imgKey),
		Body:   img,
	}); err != nil {
		return err
	}

	return nil
}

// TINYAPI

type tinyAPIClient struct {
	apiUsername, apiPassword string
	httpClient               *http.Client
}

func NewTinyAPIClient(apiKey string) *tinyAPIClient {
	return &tinyAPIClient{
		apiUsername: "api",
		apiPassword: apiKey,

		httpClient: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (tac *tinyAPIClient) newRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Host", apiHost)
	req.SetBasicAuth(tac.apiUsername, tac.apiPassword)

	return req, nil
}

func (tac *tinyAPIClient) CompressImage(img io.Reader) (string, error) {
	log.Info("sending compression request")

	req, err := tac.newRequest(http.MethodPost, fmt.Sprintf("https://%s/shrink", apiHost), img)
	if err != nil {
		return "", err
	}

	resp, err := tac.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("status code is %d, not %d", resp.StatusCode, http.StatusCreated)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var respJSON struct {
		Input struct {
			Size int `json:"size"`
		} `json:"input"`
		Output struct {
			Size int `json:"size"`
		} `json:"output"`
	}
	json.Unmarshal(respBytes, &respJSON)
	log.WithFields(log.Fields{
		"imgInputSize":  respJSON.Input.Size,
		"imgOutputSize": respJSON.Output.Size,
	}).Info("image compressed")

	loc, err := resp.Location()
	if err != nil {
		return "", nil
	}

	return loc.String(), nil
}

func (tac *tinyAPIClient) ResizeCompressedImage(loc string) (io.ReadCloser, error) {
	log.Info("sending resize request")

	var body struct {
		Resize struct {
			Method string `json:"method"`
			Width  int    `json:"width"`
		} `json:"resize"`
	}

	body.Resize.Method = "scale"
	body.Resize.Width = 640

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := tac.newRequest(http.MethodPost, loc, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := tac.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code is %d, not %d", resp.StatusCode, http.StatusOK)
	}

	log.WithFields(log.Fields{
		"imgWidth":  resp.Header.Get("Image-Width"),
		"imgHeight": resp.Header.Get("Image-Height"),
	}).Info("compressed image resized")

	return resp.Body, nil
}

func main() {
	lambda.Start(Handler)
}
