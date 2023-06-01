package controller

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func LongestDurationMovies(w http.ResponseWriter, r *http.Request) error {
	// Get the database connection.
	uname := os.Getenv("MYSQL_ROOT_USER")
	pswd := os.Getenv("MYSQL_ROOT_PASSWORD")
	sqlConnect := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", uname, pswd, "localhost", "3306", "test")
	db, err := sql.Open("mysql", sqlConnect)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Get the top 10 movies with the longest runtime.
	sql := `SELECT tconst, primaryTitle, runtimeMinutes, genres
			FROM movies
			ORDER BY runtimeMinutes DESC
			LIMIT 10;`
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Iterate over the rows and create a JSON response.
	var movies []struct {
		Tconst         string `json:"tconst"`
		PrimaryTitle   string `json:"primaryTitle"`
		RuntimeMinutes int    `json:"runtimeMinutes"`
		Genres         string `json:"genres"`
	}
	for rows.Next() {
		var movie struct {
			Tconst         string `json:"tconst"`
			PrimaryTitle   string `json:"primaryTitle"`
			RuntimeMinutes int    `json:"runtimeMinutes"`
			Genres         string `json:"genres"`
		}
		err := rows.Scan(&movie.Tconst, &movie.PrimaryTitle, &movie.RuntimeMinutes, &movie.Genres)
		if err != nil {
			panic(err)
		}
		movies = append(movies, movie)
	}

	return json.NewEncoder(w).Encode(movies)
}
func NewMovie(w http.ResponseWriter, r *http.Request) error {
	// Get the database connection.
	db, err := sql.Open("mysql", "sourabh:Iamin@tcp(localhost:3306)/test")
	if err != nil {
		panic(err)
	}

	// Read the JSON request body.
	var movie struct {
		Tconst         string `json:"tconst"`
		TitleType      string `json:"titleType"`
		PrimaryTitle   string `json:"primaryTitle"`
		RuntimeMinutes int    `json:"runtimeMinutes"`
		Genres         string `json:"genres"`
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	umr := json.Unmarshal(data, &movie)
	if umr != nil {
		panic(umr)
	}

	// Insert the movie into the database.
	sql := `INSERT INTO movies (tconst, titleType, primaryTitle, runtimeMinutes, genres)
			VALUES	 (?, ?, ?, ?, ?)`
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(movie.Tconst, movie.TitleType, movie.PrimaryTitle, movie.RuntimeMinutes, movie.Genres)
	if err != nil {
		panic(err)
	}

	response := struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}{
		Success: true,
		Message: "Movie successfully added.",
	}

	// Write the success message.
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

	return nil

}

func TopRatedMovies(w http.ResponseWriter, r *http.Request) error {
	// Get the database connection.
	db, err := sql.Open("mysql", "sourabh:Iamin@tcp(localhost:3306)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Get the top 10 movies with the highest average rating.
	sql := `SELECT tconst, primaryTitle, genres, averageRating
			FROM ratings
			WHERE averageRating > 6.0
			ORDER BY averageRating DESC
			LIMIT 10;`
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Iterate over the rows and create a JSON response.
	var movies []struct {
		Tconst        string  `json:"tconst"`
		PrimaryTitle  string  `json:"primaryTitle"`
		Genres        string  `json:"genres"`
		AverageRating float64 `json:"averageRating"`
	}
	for rows.Next() {
		var movie struct {
			Tconst        string  `json:"tconst"`
			PrimaryTitle  string  `json:"primaryTitle"`
			Genres        string  `json:"genres"`
			AverageRating float64 `json:"averageRating"`
		}
		err := rows.Scan(&movie.Tconst, &movie.PrimaryTitle, &movie.Genres, &movie.AverageRating)
		if err != nil {
			panic(err)
		}
		movies = append(movies, movie)
	}

	// Write the JSON response.
	response := struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Movies  []struct {
			Tconst        string  `json:"tconst"`
			PrimaryTitle  string  `json:"primaryTitle"`
			Genres        string  `json:"genres"`
			AverageRating float64 `json:"averageRating"`
		} `json:"movies"`
	}{
		Success: true,
		Message: "Movies successfully retrieved.",
		Movies:  movies,
	}
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		panic(err)
	}

	return nil
}

func GenreMoviesWithSubtotals(w http.ResponseWriter, r *http.Request) error {
	db, err := sql.Open("mysql", "sourabh:Iamin@tcp(localhost:3306)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Get the movies genre-wise with Subtotals of their numVotes
	stmt, err := db.Prepare("SELECT genres, COUNT(1) AS numVotes FROM movies GROUP BY genres")
	if err != nil {
		log.Fatal(err)
	}

	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}

	var genres []struct {
		Genre    string `json:"genre"`
		NumVotes int    `json:"numVotes"`
	}
	for rows.Next() {
		var genre struct {
			Genre    string `json:"genre"`
			NumVotes int    `json:"numVotes"`
		}
		err := rows.Scan(&genre.Genre, &genre.NumVotes)
		if err != nil {
			log.Fatal(err)
		}

		genres = append(genres, genre)
	}
	// Write the movies to the response body
	json.NewEncoder(w).Encode(genres)
	return nil
}

func UpdateRuntimeMinutes(w http.ResponseWriter, r *http.Request) {

	db, err := sql.Open("mysql", "sourabh:Iamin@tcp(localhost:3306)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	stmt, err := db.Prepare("UPDATE movies SET runtimeMinutes = runtimeMinutes + CASE genres WHEN 'Documentary' THEN 15 WHEN 'Animation' THEN 30 ELSE 45 END")
	if err != nil {
		log.Fatal(err)
	}

	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	var response struct {
		Message string `json:"message"`
	}
	response.Message = "Success!"
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
