package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"onito-movie/controller"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/labstack/echo/v4"
)

func main() {
	// Create a new database connection.
	uname := os.Getenv("MYSQL_ROOT_USER")
	pswd := os.Getenv("MYSQL_ROOT_PASSWORD")
	host := "localhost"
	port := "3306"
	dbname := "test"
	sqlConnect := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", uname, pswd, host, port, dbname)
	db, err := sql.Open("mysql", sqlConnect)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create the `movies` table.
	sql := `CREATE TABLE movies (
		id INT NOT NULL AUTO_INCREMENT,
		tconst VARCHAR(10) NOT NULL,
		titleType VARCHAR(10) NOT NULL,
		primaryTitle VARCHAR(255) NOT NULL,
		runtimeMinutes INT NOT NULL,
		genres VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	_, err = db.Exec(sql)
	if err != nil {
		panic(err)
	}
	// Create the `ratings` table.
	sql = `CREATE TABLE ratings (
		id INT NOT NULL AUTO_INCREMENT,
		tconst VARCHAR(10) NOT NULL,
		averageRating DECIMAL(4,1) NOT NULL,
		numVotes INT NOT NULL,
		PRIMARY KEY (id)
	)`
	_, err = db.Exec(sql)
	if err != nil {
		panic(err)
	}

	// Read the CSV data into the `movies` table.
	csvFile, err := os.Open("movies.csv")
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		// Skip the header row.
		if row[0] == "tconst" {
			continue
		}

		// Insert the row into the `movies` table.
		sql := `INSERT INTO movies (tconst, titleType, primaryTitle, runtimeMinutes, genres)
				VALUES (?, ?, ?, ?, ?)`
		stmt, err := db.Prepare(sql)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		_, err = stmt.Exec(row[0], row[1], row[2], row[3], row[4])
		if err != nil {
			panic(err)
		}
	}

	// Read the CSV data into the `ratings` table.
	csvFile, err = os.Open("ratings.csv")
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	reader = csv.NewReader(csvFile)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		// Insert the row into the `ratings` table.
		sql := `INSERT INTO ratings (tconst, averageRating, numVotes)
				VALUES (?, ?, ?)`
		stmt, err := db.Prepare(sql)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		averageRating, _ := strconv.ParseFloat(row[1], 64)

		numVotes, _ := strconv.ParseInt(row[2], 10, 64)

		_, err = stmt.Exec(row[0], averageRating, numVotes)
		if err != nil {
			panic(err)
		}
	}

	// Print a success message.
	fmt.Println("Done!")

	e := echo.New()

	e.GET("/", func(ctx echo.Context) error {
		return ctx.String(200, "Hello, world!")
	})

	e.GET("/api/v1/longest-duration-movies", func(ctx echo.Context) error {

		longestMovies := controller.LongestDurationMovies(ctx.Response().Writer, ctx.Request())
		return ctx.JSON(http.StatusOK, longestMovies)
	})

	e.POST("/api/v1/new-movie", func(ctx echo.Context) error {

		controller.NewMovie(ctx.Response().Writer, ctx.Request())
		return ctx.JSON(http.StatusOK, ctx.Request().Response)
	})

	e.GET("/api/v1/top-rated-movies", func(ctx echo.Context) error {

		topratedmovie := controller.TopRatedMovies(ctx.Response().Writer, ctx.Request())
		return ctx.JSON(http.StatusOK, topratedmovie)
	})

	e.GET("/api/v1/genre-movies-with-subtotals", func(ctx echo.Context) error {
		controller.GenreMoviesWithSubtotals(ctx.Response().Writer, ctx.Request())
		return ctx.JSON(http.StatusOK, ctx.Request().Response)
	})

	e.POST("/api/v1/update-runtime-minutes", func(ctx echo.Context) error {
		controller.UpdateRuntimeMinutes(ctx.Response().Writer, ctx.Request())
		return ctx.JSON(http.StatusOK, ctx.Response().Writer)
	})
	e.Logger.Fatal(e.Start(":8000"))

}
