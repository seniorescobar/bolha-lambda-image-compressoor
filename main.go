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
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/seniorescobar/bolha/lambda/common"

	log "github.com/sirupsen/logrus"
)

const (
	apiHost = "api.tinify.com"
)

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
			Size int    `json:"size"`
			Type string `json:"type"`
		} `json:"input"`
	}
	json.Unmarshal(respBytes, &respJSON)
	log.WithField("imgSizeCompressed", respJSON.Input.Size).Info("image compressed")

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

func Handler(ctx context.Context, event events.S3Event) error {
	log.Info("compressing images")

	sess := session.Must(session.NewSession())

	s3Client := common.NewS3Client(sess)

	tac := NewTinyAPIClient(os.Getenv("TINYAPIKEY"))

	for _, record := range event.Records {
		s3Obj := record.S3.Object

		imgKey, imgSize := s3Obj.Key, s3Obj.Size

		log.WithFields(log.Fields{
			"imgKey":  imgKey,
			"imgSize": imgSize,
		}).Info("downloading image")

		img, err := s3Client.DownloadImage(imgKey)
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

		if err := s3Client.UploadImage(strings.Replace(imgKey, "_uncompressed.jpg", "_compressed.jpg", 1), imgNew); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	lambda.Start(Handler)
}
