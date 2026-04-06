package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	apiURL      = "https://num.voxlink.ru/get/"
	timeout     = 5 * time.Second
	rpsLimit    = 10
	workerCount = 5
)

type VoxlinkResponse struct {
	Status   string `json:"status"`
	Operator string `json:"operator"`
	Region   string `json:"region"`
	Code     string `json:"code"`
	Number   string `json:"number"`
	Error    string `json:"error"`
}

type Result struct {
	Number   string
	Status   string
	Operator string
	Region   string
	Code     string
	Full     string
	Error    string
}

func main() {

	inputFile, err := os.Open("input.csv")
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create("output.csv")
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	reader := csv.NewReader(inputFile)
	writer := csv.NewWriter(outputFile)

	// header
	writer.Write([]string{
		"Номер",
		"Статус",
		"Оператор",
		"Регион",
		"Код",
		"Полный номер",
		"Ошибка",
	})

	client := &http.Client{
		Timeout: timeout,
	}

	limiter := rate.NewLimiter(rate.Limit(rpsLimit), rpsLimit)

	jobs := make(chan string)
	results := make(chan Result)

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(client, limiter, jobs, results, &wg)
	}

	// reader goroutine
	go func() {
		for {
			row, err := reader.Read()

			if err == io.EOF {
				break
			}

			if err != nil {
				results <- Result{
					Error: err.Error(),
				}
				continue
			}

			if len(row) == 0 {
				continue
			}

			number := strings.TrimSpace(row[0])
			if number == "" {
				continue
			}

			jobs <- number
		}

		close(jobs)
	}()

	// close results when workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {

		writer.Write([]string{
			r.Number,
			r.Operator,
			r.Region,
			r.Code,
			r.Full,
		})
	}

	writer.Flush()
}

func worker(client *http.Client, limiter *rate.Limiter, jobs <-chan string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for number := range jobs {

		ctx := context.Background()

		if err := limiter.Wait(ctx); err != nil {
			results <- Result{
				Number: number,
				Error:  err.Error(),
			}
			continue
		}

		res := processNumber(client, number)

		results <- res
	}
}

func processNumber(client *http.Client, number string) Result {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	u, _ := url.Parse(apiURL)
	q := u.Query()
	q.Set("num", number)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return Result{
			Number: number,
			Error:  err.Error(),
		}
	}

	req.Header.Set("User-Agent", "voxlink-go-client/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return Result{
			Number: number,
			Error:  err.Error(),
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{
			Number: number,
			Error:  err.Error(),
		}
	}

	if resp.StatusCode != http.StatusOK {
		return Result{
			Number: number,
			Error:  fmt.Sprintf("http %d: %s", resp.StatusCode, string(body)),
		}
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		return Result{
			Number: number,
			Error:  fmt.Sprintf("unexpected content-type: %s body: %s", contentType, string(body)),
		}
	}

	var apiResp VoxlinkResponse

	err = json.Unmarshal(body, &apiResp)
	if err != nil {
		return Result{
			Number: number,
			Error:  fmt.Sprintf("json parse error: %v body: %s", err, string(body)),
		}
	}

	return Result{
		Number:   number,
		Status:   apiResp.Status,
		Operator: apiResp.Operator,
		Region:   apiResp.Region,
		Code:     apiResp.Code,
		Full:     apiResp.Number,
		Error:    apiResp.Error,
	}
}
