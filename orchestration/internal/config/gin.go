package config

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"orchestration/pkg/helpers"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func panicRecovery() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[!!!Panic Recovery!!!] %s \"%s\" Error: +%v", ctx.Request.Method, ctx.Request.RequestURI, err)
			}

			ctx.JSON(http.StatusInternalServerError, gin.H{
				"status": false,
				"error":  "Internal Server Error",
			})

			ctx.Abort()
		}()

		ctx.Next()
	}
}

func setCors(app *gin.Engine) {
	origins := os.Getenv("CORS_ALLOW_ORIGINS")
	methods := os.Getenv("CORS_ALLOW_METHODS")
	headers := os.Getenv("CORS_ALLOW_HEADERS")
	allowCredentialEnv := os.Getenv("CORS_ALLOW_CREDENTIALS")
	allowCredential := false

	allowWildcardEnv := os.Getenv("CORS_ALLOW_ORIGIN_WILDCARD")
	allowWildcard := true

	if v, err := strconv.ParseBool(allowCredentialEnv); err == nil {
		allowCredential = v
	}

	if v, err := strconv.ParseBool(allowWildcardEnv); err == nil {
		allowWildcard = v
	}

	app.Use(cors.New(cors.Config{
		AllowOrigins:     helpers.TrimSlice(strings.Split(origins, ",")),
		AllowMethods:     helpers.TrimSlice(strings.Split(methods, ",")),
		AllowHeaders:     helpers.TrimSlice(strings.Split(headers, ",")),
		AllowCredentials: allowCredential,
		AllowWildcard:    allowWildcard,
	}))
}

func NewGin() *gin.Engine {
	app := gin.New()

	setCors(app)

	app.Use(panicRecovery())

	app.NoRoute(func(ctx *gin.Context) {
		ctx.JSON(http.StatusNotFound, gin.H{
			"status": false,
			"error":  fmt.Sprintf("%s Not Found", ctx.Request.URL),
		})
	})

	return app
}
