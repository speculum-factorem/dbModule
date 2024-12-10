package main

import (
    "database/sql"
    "fmt"
    "io/ioutil"
    "log"

    _ "github.com/mattn/go-sqlite3"
    "gopkg.in/yaml.v2"
)

// User представляет пользователя.
type User struct {
    ID       int
    Name     string
    Lastname string
    Password string
    Email    string
    Phone    string
}

// Restaurant представляет ресторан.
type Restaurant struct {
    ID            int
    Name          string
    Type          string
    Keys          string
    AveragePrice  int
    UserID        int
}

// Database обрабатывает соединение с БД и операции с ней
type Database struct {
    *sql.DB
}

// Queries содержит SQL-запросы
type Queries struct {
	DropUser          string yaml:"drop_user"
	DropRestaurants   string yaml:"drop_restaurants"
	CreateUser        string yaml:"create_user"
	CreateRestaurants string yaml:"create_restaurants"
	InsertUser        string yaml:"insert_user"
	InsertRestaurant  string yaml:"insert_restaurant"
	SelectUsers       string yaml:"select_users"
	SelectRestaurants string yaml:"select_restaurants"
	SelectJoin        string yaml:"select_join"
}

// NewDatabase создает новое соединение с БД
func NewDatabase(dataSourceName string) (*Database, error) {
    db, err := sql.Open("sqlite3", dataSourceName)
    if err != nil {
        return nil, err
    }
    return &Database{db}, nil
}

// LoadQueries загружает SQL-запросы из YAML файла
func LoadQueries(filename string) (Queries, error) {
    var queries Queries
    data, err := ioutil.ReadFile(filename)
    if err != nil {
        return queries, err
    }
    
    err = yaml.Unmarshal(data, &queries)
    return queries, err
}

// Initialize создает таблицы в базе данных
func (db *Database) Initialize(queries Queries) error {
    statements := []string{
        queries.DropUser,
        queries.DropRestaurants,
        queries.CreateUser,
        queries.CreateRestaurants,
    }

    for _, statement := range statements {
        if _, err := db.Exec(statement); err != nil {
            return err
        }
    }
    return nil
}

// InsertUser добавляет пользователя в базу данных
func (db *Database) InsertUser(user User, query string) error {
    statement, err := db.Prepare(query)
    if err != nil {
        return err
    }
    _, err = statement.Exec(user.Name, user.Lastname, user.Password, user.Email, user.Phone)
    return err
}

// InsertRestaurant добавляет ресторан в базу данных
func (db *Database) InsertRestaurant(restaurant Restaurant, query string) error {
    statement, err := db.Prepare(query)
    if err != nil {
        return err
    }
    _, err = statement.Exec(restaurant.Name, restaurant.Type, restaurant.Keys, restaurant.AveragePrice, restaurant.UserID)
    return err
}

// SelectUsers выбирает всех пользователей из базы данных
func (db *Database) SelectUsers(query string) ([]User, error) {
    rows, err := db.Query(query)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var users []User
    for rows.Next() {
        var user User
        if err := rows.Scan(&user.ID, &user.Name, &user.Lastname, &user.Password, &user.Email, &user.Phone); err != nil {
            return nil, err
        }
        users = append(users, user)
    }
    return users, nil
}

// SelectRestaurants выбирает все рестораны из базы данных
func (db *Database) SelectRestaurants(query string) ([]Restaurant, error) {
    rows, err := db.Query(query)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var restaurants []Restaurant
    for rows.Next() {
        var restaurant Restaurant
        if err := rows.Scan(&restaurant.ID, &restaurant.Name, &restaurant.Type, &restaurant.Keys, &restaurant.AveragePrice); err != nil {
            return nil, err
        }
        restaurants = append(restaurants, restaurant)
    }
    return restaurants, nil
}

// SelectJoin выбирает данные из обеих таблиц с объединением
func (db *Database) SelectJoin(query string) ([]struct {
    UserID         int
    UserName       string
    UserLastname   string
    RestaurantID   int
    RestaurantName string
    Type           string
    AveragePrice   int
}, error) {
    rows, err := db.Query(query)
    
    if err != nil {
        return nil, err
    }
    
    defer rows.Close()

    var results []struct {
        UserID         int
        UserName       string
        UserLastname   string
        RestaurantID   int
        RestaurantName string
        Type           string
        AveragePrice   int
    }

    for rows.Next() {
        var result struct {
            UserID         int
            UserName       string
            UserLastname   string
            RestaurantID   int
            RestaurantName string
            Type           string
            AveragePrice   int
        }
        
        if err := rows.Scan(&result.UserID, &result.UserName, &result.UserLastname,
                            &result.RestaurantID, &result.RestaurantName,
                            &result.Type, &result.AveragePrice); err != nil {
            return nil, err
        }
        
        results = append(results, result)
    }
    
    return results, nil
}

func main() {
    database, err := NewDatabase("./project.db")
    
    if err != nil {
        log.Fatalf("Error opening database: %v", err)
    }

    queries, err := LoadQueries("./config/queries.yaml")
    
    if err != nil {
        log.Fatalf("Error loading queries: %v", err)
    }

    if err := database.Initialize(queries); err != nil {
        log.Fatalf("Error initializing database: %v", err)
    }

    // Пример добавления пользователей и ресторанов
    user := User{Name: "lorem", Lastname: "lorem", Password: "lorem", Email: "lorem@example.com", Phone: "+88888888888"}
    
    if err := database.InsertUser(user, queries.InsertUser); err != nil {
        log.Fatalf("Error inserting user: %v", err)
    }

    restaurant := Restaurant{Name: "ipsum", Type: "ipsum", Keys: "ipsum", AveragePrice: 2, UserID: 1}
    
    if err := database.InsertRestaurant(restaurant, queries.InsertRestaurant); err != nil {
        log.Fatalf("Error inserting restaurant: %v", err)
    }

    // Выборка пользователей и ресторанов
    users, _ := database.SelectUsers(queries.SelectUsers)
    
    for _, u := range users {
        fmt.Printf("User: %d %s %sn", u.ID, u.Name, u.Lastname)
    }

    restaurants, _ := database.SelectRestaurants(queries.SelectRestaurants)
    
    for _, r := range restaurants {
        fmt.Printf("Restaurant: %d %s %sn", r.ID, r.Name, r.Type)
    }

    // Join выборка
    joinResults, _ := database.SelectJoin(queries.SelectJoin)
    
    for _, result := range joinResults {
        fmt.Printf("User ID: %d | Name: %s %s | Restaurant ID: %d | Restaurant Name: %s | Type: %s | Average Price: %dn",
            result.UserID, result.UserName, result.UserLastname,
            result.RestaurantID, result.RestaurantName,
            result.Type, result.AveragePrice)
    }
}