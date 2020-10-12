package edgedb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Person struct {
	FirstName string `edgedb:"first_name"`
	LastName  string `edgedb:"last_name"`
}

type Movie struct {
	Title    string   `edgedb:"title"`
	Year     int64    `edgedb:"year"`
	Director Person   `edgedb:"director"`
	Actors   []Person `edgedb:"actors"`
}

func TestTutorial(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dbName := fmt.Sprintf("test%v", rand.Intn(10_000))
	err := conn.Execute("CREATE DATABASE " + dbName)
	require.Nil(t, err)

	defer func() {
		conn.Execute("DROP DATABASE " + dbName + ";")
	}()

	opts := Options{Database: dbName, User: "edgedb", Host: "localhost"}
	edb, _ := Connect(opts)
	defer edb.Close()

	err = edb.Execute(`
		START MIGRATION TO {
			module default {
				type Movie {
					required property title -> str;
					# the year of release
					property year -> int64;
					required link director -> Person;
					multi link actors -> Person;
				}
				type Person {
					required property first_name -> str;
					required property last_name -> str;
				}
			}
		};

		POPULATE MIGRATION;

		COMMIT MIGRATION;
	`)
	require.Nil(t, err)

	err = edb.Execute(`
		INSERT Movie {
			title := 'Blade Runner 2049',
			year := 2017,
			director := (
				INSERT Person {
					first_name := 'Denis',
					last_name := 'Villeneuve',
				}
			),
			actors := {
				(INSERT Person {
					first_name := 'Harrison',
					last_name := 'Ford',
				}),
				(INSERT Person {
					first_name := 'Ryan',
					last_name := 'Gosling',
				}),
				(INSERT Person {
					first_name := 'Ana',
					last_name := 'de Armas',
				}),
			}
		}`,
	)
	require.Nil(t, err)

	err = edb.Execute(`
		INSERT Movie {
				title := 'Dune',
				director := (
						SELECT Person
						FILTER
								# the last name is sufficient
								# to identify the right person
								.last_name = 'Villeneuve'
						# the LIMIT is needed to satisfy the single
						# link requirement validation
						LIMIT 1
				)
		};`,
	)
	require.Nil(t, err)

	var out []Movie
	err = edb.Query(`
		SELECT Movie {
				title,
				year,
				director: {
						first_name,
						last_name
				},
				actors: {
						first_name,
						last_name
				}
		}`,
		&out,
	)
	require.Nil(t, err)

	expected := []Movie{
		Movie{
			Title: "Blade Runner 2049",
			Year:  int64(2017),
			Director: Person{
				FirstName: "Denis",
				LastName:  "Villeneuve",
			},
			Actors: []Person{
				Person{
					FirstName: "Harrison",
					LastName:  "Ford",
				},
				Person{
					FirstName: "Ryan",
					LastName:  "Gosling",
				},
				Person{
					FirstName: "Ana",
					LastName:  "de Armas",
				},
			},
		},
		Movie{
			Title: "Dune",
			Director: Person{
				FirstName: "Denis",
				LastName:  "Villeneuve",
			},
			Actors: []Person{},
		},
	}

	assert.Equal(t, expected, out)
}
