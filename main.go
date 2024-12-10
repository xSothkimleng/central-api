package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// Define storage servers and database connection details
var storageServers = map[string]string{
	"Singapore": "http://35.197.153.160:5001",
	"New York":  "http://34.174.158.135:5002",
	"London":    "http://34.147.235.178:5003",
}

var db *sql.DB

// Initialize the database connection
func initDB() {
    var err error
    // Replace with your actual credentials
    dsn := "ksoth:password@tcp(35.186.153.162:3306)/cdn_database"
    db, err = sql.Open("mysql", dsn)
    if err != nil {
        panic(fmt.Sprintf("Failed to connect to database: %v", err))
    }

    // Test the connection
    if err := db.Ping(); err != nil {
        panic(fmt.Sprintf("Failed to ping database: %v", err))
    }

    fmt.Println("Connected to the database successfully!")
}

// Add file URLs to the database
func addFileToDatabase(filename, singaporeURL, newyorkURL, londonURL string) error {
	query := "INSERT INTO file_urls (filename, singapore_url, newyork_url, london_url) VALUES (?, ?, ?, ?)"
	_, err := db.Exec(query, filename, singaporeURL, newyorkURL, londonURL)
	return err
}

// Get file URLs from the database
func getFileFromDatabase(filename string) (map[string]string, error) {
	query := "SELECT singapore_url, newyork_url, london_url FROM file_urls WHERE filename = ?"
	row := db.QueryRow(query, filename)

	var singaporeURL, newyorkURL, londonURL string
	err := row.Scan(&singaporeURL, &newyorkURL, &londonURL)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"Singapore": singaporeURL,
		"New York":  newyorkURL,
		"London":    londonURL,
	}, nil
}

// Delete file URLs from the database
func deleteFileFromDatabase(filename string) error {
	query := "DELETE FROM file_urls WHERE filename = ?"
	_, err := db.Exec(query, filename)
	return err
}

// Upload file handler
func uploadHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Uploading file...")

    // Parse the uploaded file
    file, header, err := r.FormFile("file")
    if err != nil {
        fmt.Printf("Error parsing form file: %v\n", err)
        http.Error(w, "Invalid file upload", http.StatusBadRequest)
        return
    }
    defer file.Close()

    // Log file details for debugging
    fmt.Printf("Uploaded File: %s\n", header.Filename)
    fmt.Printf("MIME Header: %v\n", header.Header)

    // Create a temporary file buffer to replicate across all servers
    var tempBuffer bytes.Buffer
    _, err = io.Copy(&tempBuffer, file)
    if err != nil {
        fmt.Printf("Error reading file into buffer: %v\n", err)
        http.Error(w, "Failed to read file", http.StatusInternalServerError)
        return
    }

    // Store URLs for database update
    fileURLs := make(map[string]string)

    // Replicate file to all storage servers
	for location, serverURL := range storageServers {
		// Create a multipart form body
		var buffer bytes.Buffer
		writer := multipart.NewWriter(&buffer)
	
		// Add the file to the multipart form
		part, err := writer.CreateFormFile("file", header.Filename)
		if err != nil {
			fmt.Printf("Error creating form file for %s: %v\n", location, err)
			http.Error(w, fmt.Sprintf("Failed to upload to %s", location), http.StatusInternalServerError)
			return
		}
	
		// Write the file content into the form
		_, err = io.Copy(part, bytes.NewReader(tempBuffer.Bytes()))
		if err != nil {
			fmt.Printf("Error writing file to form for %s: %v\n", location, err)
			http.Error(w, fmt.Sprintf("Failed to upload to %s", location), http.StatusInternalServerError)
			return
		}
	
		// Close the multipart writer to finalize the body
		err = writer.Close()
		if err != nil {
			fmt.Printf("Error closing multipart writer for %s: %v\n", location, err)
			http.Error(w, fmt.Sprintf("Failed to upload to %s", location), http.StatusInternalServerError)
			return
		}
	
		// Send the POST request with the multipart body
		req, err := http.NewRequest("POST", serverURL+"/upload", &buffer)
		if err != nil {
			fmt.Printf("Error creating request for %s: %v\n", location, err)
			http.Error(w, fmt.Sprintf("Failed to upload to %s", location), http.StatusInternalServerError)
			return
		}
	
		// Set the Content-Type header to the correct multipart boundary
		req.Header.Set("Content-Type", writer.FormDataContentType())
	
		// Execute the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error sending request to %s: %v\n", location, err)
			http.Error(w, fmt.Sprintf("Failed to upload to %s", location), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
	
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Non-OK response from %s: %s\n", location, resp.Status)
			http.Error(w, fmt.Sprintf("Failed to upload to %s", location), http.StatusInternalServerError)
			return
		}
	
		// Save the file URL for database update
		fileURLs[location] = serverURL + "/files/" + header.Filename
	}
	

    // Update the database with file URLs
    err = addFileToDatabase(header.Filename, fileURLs["Singapore"], fileURLs["New York"], fileURLs["London"])
    if err != nil {
        fmt.Printf("Error updating database: %v\n", err)
        http.Error(w, "Failed to add file to database", http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
    fmt.Fprintln(w, "File uploaded and replicated successfully")
}

// Retrieve all files handler
func getAllFilesHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Getting all files for the nearest region...")

    // Query the database for all file entries
    query := "SELECT filename, singapore_url, newyork_url, london_url FROM file_urls"
    rows, err := db.Query(query)
    if err != nil {
        fmt.Printf("Error retrieving files from database: %v\n", err)
        http.Error(w, "Failed to retrieve files from database", http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    // Get the client's IP address
    clientIP := getClientIP(r)
    fmt.Printf("Client IP: %s\n", clientIP)

    // Determine the client's nearest region
    serverCoordinates := map[string][2]float64{
        "Singapore": {1.3521, 103.8198},
        "New York":  {40.7128, -74.0060},
        "London":    {51.5074, -0.1278},
    }

    clientCoordinates, err := getCoordinatesFromIP(clientIP)
    if err != nil {
        fmt.Printf("Error determining client coordinates: %v\n", err)
        http.Error(w, "Failed to determine client location", http.StatusInternalServerError)
        return
    }

    nearestRegion := ""
    shortestDistance := float64(^uint(0) >> 1) // Max float
    for region, coords := range serverCoordinates {
        distance := haversineDistance(clientCoordinates, coords)
        if distance < shortestDistance {
            shortestDistance = distance
            nearestRegion = region
        }
    }

    if nearestRegion == "" {
        fmt.Println("Failed to determine the nearest region")
        http.Error(w, "Could not determine nearest region", http.StatusInternalServerError)
        return
    }
    fmt.Printf("Nearest region: %s\n", nearestRegion)

    // Prepare a list of files for the nearest region
    files := []map[string]string{}
    for rows.Next() {
        var filename, singaporeURL, newyorkURL, londonURL string
        if err := rows.Scan(&filename, &singaporeURL, &newyorkURL, &londonURL); err != nil {
            fmt.Printf("Error scanning file row: %v\n", err)
            continue
        }

        // Select the URL based on the nearest region
        var url string
        switch nearestRegion {
        case "Singapore":
            url = singaporeURL
        case "New York":
            url = newyorkURL
        case "London":
            url = londonURL
        }

        files = append(files, map[string]string{
            "filename": filename,
            "url":      url,
        })
    }

    // Check for errors during iteration
    if err := rows.Err(); err != nil {
        fmt.Printf("Error iterating over file rows: %v\n", err)
        http.Error(w, "Failed to process files from database", http.StatusInternalServerError)
        return
    }

    // Respond with the list of files for the nearest region
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(files)
}

// Retrieve file handler
func getFileHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Getting file...")

    // Get the filename from query parameters
    filename := r.URL.Query().Get("filename")
    if filename == "" {
        http.Error(w, "Filename is required", http.StatusBadRequest)
        return
    }

    // Query the database for file URLs
    urls, err := getFileFromDatabase(filename)
    if err != nil {
        fmt.Printf("Error retrieving file from database: %v\n", err)
        http.Error(w, "File not found in database", http.StatusNotFound)
        return
    }

    // Log the retrieved URLs
    fmt.Printf("Retrieved URLs for %s: %v\n", filename, urls)

    // Get the client's IP address
    clientIP := getClientIP(r)
    fmt.Printf("Client IP: %s\n", clientIP)

    // Find the nearest server
    nearestServer, nearestURL := findNearestServer(clientIP, urls)
    if nearestServer == "" {
        fmt.Printf("Failed to determine the nearest server for file: %s\n", filename)
        http.Error(w, "No suitable servers found for the file", http.StatusInternalServerError)
        return
    }

    // Respond with the URL for the nearest server
    response := map[string]string{
        "filename": filename,
        "region":   nearestServer,
        "url":      nearestURL,
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// Helper function to get client IP address
func getClientIP(r *http.Request) string {
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		// Take the first IP if there are multiple
		return strings.Split(xForwardedFor, ",")[0]
	}

	// Fallback to the remote address
	clientIP := r.RemoteAddr
	if colon := strings.LastIndex(clientIP, ":"); colon != -1 {
		clientIP = clientIP[:colon]
	}
	return clientIP
}


// Helper function to find the nearest server
func findNearestServer(clientIP string, urls map[string]string) (string, string) {
    // Define the coordinates of your storage servers
    serverCoordinates := map[string][2]float64{
        "Singapore": {1.3521, 103.8198},
        "New York":  {40.7128, -74.0060},
        "London":    {51.5074, -0.1278},
    }

    // Simulated function to get coordinates from client IP
    clientCoordinates, err := getCoordinatesFromIP(clientIP)
    if err != nil {
        fmt.Printf("Error determining client coordinates: %v\n", err)
        return "", ""
    }

    // Find the nearest server
    var nearestServer string
    var nearestURL string
    shortestDistance := float64(^uint(0) >> 1) // Set to max float

    for server, coords := range serverCoordinates {
        distance := haversineDistance(clientCoordinates, coords)
        if distance < shortestDistance {
            shortestDistance = distance
            nearestServer = server
            nearestURL = urls[server]
        }
    }

    return nearestServer, nearestURL
}

// Simulated function to get coordinates from IP (replace with a real geolocation service)
type GeolocationResponse struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// getCoordinatesFromIP fetches the geographical coordinates of the given IP
// getCoordinatesFromIP fetches the geographical coordinates of the given IP
func getCoordinatesFromIP(clientIP string) ([2]float64, error) {
	// ip-api.com endpoint
	apiURL := fmt.Sprintf("http://ip-api.com/json/%s", clientIP)

	fmt.Printf("Fetching geolocation for IP: %s\n", clientIP)

	// Make an HTTP GET request
	resp, err := http.Get(apiURL)
	if err != nil {
		return [2]float64{}, fmt.Errorf("failed to fetch geolocation: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response status is OK
	if resp.StatusCode != http.StatusOK {
		return [2]float64{}, fmt.Errorf("failed to fetch geolocation, status: %v", resp.Status)
	}

	// Decode the JSON response
	var geoResponse map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&geoResponse); err != nil {
		return [2]float64{}, fmt.Errorf("failed to decode geolocation response: %v", err)
	}

	// Debug the full response
	fmt.Printf("Geolocation response: %+v\n", geoResponse)

	// Extract latitude and longitude
	if lat, ok := geoResponse["lat"].(float64); ok {
		if lon, ok := geoResponse["lon"].(float64); ok {
			return [2]float64{lat, lon}, nil
		}
	}

	return [2]float64{}, fmt.Errorf("failed to extract lat/lon from geolocation response")
}

// Calculate the Haversine distance between two coordinates
func haversineDistance(coord1, coord2 [2]float64) float64 {
    const earthRadius = 6371 // Earth radius in kilometers
    lat1, lon1 := coord1[0], coord1[1]
    lat2, lon2 := coord2[0], coord2[1]

    dLat := degreesToRadians(lat2 - lat1)
    dLon := degreesToRadians(lon2 - lon1)

    a := math.Sin(dLat/2)*math.Sin(dLat/2) +
        math.Cos(degreesToRadians(lat1))*math.Cos(degreesToRadians(lat2))*
            math.Sin(dLon/2)*math.Sin(dLon/2)

    c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

    return earthRadius * c
}

// Convert degrees to radians
func degreesToRadians(degrees float64) float64 {
    return degrees * math.Pi / 180
}



// Delete file handler
func deleteHandler(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Query().Get("filename")
	if filename == "" {
		http.Error(w, "Filename is required", http.StatusBadRequest)
		return
	}

	// Delete file from all storage servers
	for location, serverURL := range storageServers {
		req, err := http.NewRequest("DELETE", serverURL+"/delete?filename="+filename, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send delete request to %s", location), http.StatusInternalServerError)
			return
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			http.Error(w, fmt.Sprintf("Failed to delete from %s", location), http.StatusInternalServerError)
			return
		}
	}

	// Update the database
	err := deleteFileFromDatabase(filename)
	if err != nil {
		http.Error(w, "Failed to delete file from database", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "File deleted successfully")
}

func main() {
	// Initialize the database connection
	initDB()

	// Define routes
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/files", getFileHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/files/all", getAllFilesHandler)

	// Start the server
	fmt.Println("Central API Server is running on port 5000...")
	http.ListenAndServe(":5000", nil)
}
