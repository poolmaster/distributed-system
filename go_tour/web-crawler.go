package main

import (
  "fmt"
  "sync"
)

type Fetcher interface {
  // Fetch returns the body of URL and
  // a slice of URLs found on that page.
  Fetch(url string) (body string, urls []string, err error)
}

type Cache struct {
  visited map[string]bool
  mux sync.Mutex
}

type Response struct {
  url string
  body string
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher,
           ch chan Response, cache Cache) {
  defer close(ch)
  // This implementation doesn't do either:
  if depth <= 0 {
    return
  }
  cache.mux.Lock()
  if cache.visited[url] {
    cache.mux.Unlock()
    return
  }
  cache.visited[url] = true
  cache.mux.Unlock()

  body, urls, err := fetcher.Fetch(url)
  if err != nil {
    fmt.Println(err)
    return
  }

  ch <- Response{url, body}
  results := make([]chan Response, len(urls))
  for i, u := range urls {
    results[i] = make(chan Response)
    go Crawl(u, depth-1, fetcher, results[i], cache)
  }
  for i := range results {
    for resp := range results[i] {
      ch <- resp
    }
  }

  return
}

func main() {
  var ch = make(chan Response)
  var cache = Cache{visited: make(map[string]bool)}
  
  go Crawl("http://golang.org/", 4, fetcher, ch, cache)
  for resp := range ch {
    fmt.Printf("found: %s %q\n", resp.url, resp.body)
  }
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
  body string
  urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
  if res, ok := f[url]; ok {
    return res.body, res.urls, nil
  }
  return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
  "http://golang.org/": &fakeResult{
    "The Go Programming Language",
    []string{
      "http://golang.org/pkg/",
      "http://golang.org/cmd/",
    },
  },
  "http://golang.org/pkg/": &fakeResult{
    "Packages",
    []string{
      "http://golang.org/",
      "http://golang.org/cmd/",
      "http://golang.org/pkg/fmt/",
      "http://golang.org/pkg/os/",
    },
  },
  "http://golang.org/pkg/fmt/": &fakeResult{
    "Package fmt",
    []string{
      "http://golang.org/",
      "http://golang.org/pkg/",
    },
  },
  "http://golang.org/pkg/os/": &fakeResult{
    "Package os",
    []string{
      "http://golang.org/",
      "http://golang.org/pkg/",
    },
  },
}

