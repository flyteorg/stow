package s3

import (
	"context"
	"fmt"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	config2 "github.com/aws/aws-sdk-go-v2/config"
	credentials2 "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/flyteorg/stow"
	"github.com/pkg/errors"
	"net/url"
)

// Kind represents the name of the location/storage type.
const Kind = "s3"

var (
	authTypeAccessKey = "accesskey"
	authTypeIAM       = "iam"
)

const (
	// ConfigAuthType is an optional argument that defines whether to use an IAM role or access key based auth
	ConfigAuthType = "auth_type"

	// ConfigAccessKeyID is one key of a pair of AWS credentials.
	ConfigAccessKeyID = "access_key_id"

	// ConfigSecretKey is one key of a pair of AWS credentials.
	ConfigSecretKey = "secret_key"

	// ConfigToken is an optional argument which is required when providing
	// credentials with temporary access.
	// ConfigToken = "token"

	// ConfigRegion represents the region/availability zone of the session.
	ConfigRegion = "region"

	// ConfigEndpoint is optional config value for changing s3 endpoint
	// used for e.g. minio.io
	ConfigEndpoint = "endpoint"

	// ConfigDisableSSL is optional config value for disabling SSL support on custom endpoints
	// Its default value is "false", to disable SSL set it to "true".
	ConfigDisableSSL = "disable_ssl"

	// ConfigV2Signing is an optional config value for signing requests with the v2 signature.
	// Its default value is "false", to enable set to "true".
	// This feature is useful for s3-compatible blob stores -- ie minio.
	ConfigV2Signing = "v2_signing"
)

func ValidateFunc(config stow.Config) error {
	authType, ok := config.Config(ConfigAuthType)
	if !ok || authType == "" {
		authType = authTypeAccessKey
	}

	if !(authType == authTypeAccessKey || authType == authTypeIAM) {
		return errors.New("invalid auth_type")
	}

	if authType == authTypeAccessKey {
		_, ok := config.Config(ConfigAccessKeyID)
		if !ok {
			return errors.New("missing Access Key ID")
		}

		_, ok = config.Config(ConfigSecretKey)
		if !ok {
			return errors.New("missing Secret Key")
		}
	}
	return nil
}

func MakeFunc(config stow.Config) (stow.Location, error) {
	authType, ok := config.Config(ConfigAuthType)
	if !ok || authType == "" {
		authType = authTypeAccessKey
	}

	if !(authType == authTypeAccessKey || authType == authTypeIAM) {
		return nil, errors.New("invalid auth_type")
	}

	if authType == authTypeAccessKey {
		_, ok := config.Config(ConfigAccessKeyID)
		if !ok {
			return nil, errors.New("missing Access Key ID")
		}

		_, ok = config.Config(ConfigSecretKey)
		if !ok {
			return nil, errors.New("missing Secret Key")
		}
	}

	// Create a new client (s3 session)
	client, endpoint, err := newS3Client(config, "")
	if err != nil {
		return nil, err
	}

	// Create a location with given config and client (s3 session).
	loc := &location{
		config:         config,
		client:         client,
		customEndpoint: endpoint,
	}

	return loc, nil
}

func init() {
	kindfn := func(u *url.URL) bool {
		return u.Scheme == Kind
	}

	stow.Register(Kind, MakeFunc, kindfn, ValidateFunc)
}

type awsEndpointResolverAdaptor func(region string, options s3.EndpointResolverOptions) (aws2.Endpoint, error)

func (a awsEndpointResolverAdaptor) ResolveEndpoint(service, region string) (aws2.Endpoint, error) {
	return a(service, s3.EndpointResolverOptions{
		ResolvedRegion: region,
	})
}

// Attempts to create a session based on the information given.
func newS3Client(config stow.Config, region string) (client *s3.Client, endpoint string, err error) {
	authType, _ := config.Config(ConfigAuthType)
	accessKeyID, _ := config.Config(ConfigAccessKeyID)
	secretKey, _ := config.Config(ConfigSecretKey)
	//	token, _ := config.Config(ConfigToken)

	if authType == "" {
		authType = authTypeAccessKey
	}

	ctx := context.Background()

	awsCfg, err := config2.LoadDefaultConfig(ctx, func(options *config2.LoadOptions) error {
		return nil
	})

	if err != nil {
		return nil, "", fmt.Errorf("failed to load AWS config. Error: %w", err)
	}

	if region == "" {
		region, _ = config.Config(ConfigRegion)
	}

	if region != "" {
		awsCfg.Region = region
	} else {
		awsCfg.Region = "us-east-1"
	}

	if authType == authTypeAccessKey {
		awsCfg.Credentials = credentials2.NewStaticCredentialsProvider(accessKeyID, secretKey, "")
	}

	endpoint, ok := config.Config(ConfigEndpoint)
	if ok {
		awsCfg.EndpointResolver = awsEndpointResolverAdaptor(s3.EndpointResolverFromURL(endpoint).ResolveEndpoint)
	}

	disableSSL, ok := config.Config(ConfigDisableSSL)
	if ok && disableSSL == "true" {
		// TODO: Implement
	}

	s3Client := s3.NewFromConfig(awsCfg)
	// , func(options *s3.Options) {
	//	options.UsePathStyle = true
	//	options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
	//		stack := middleware.NewStack("", nil)
	//
	//		// Custom Build middleware
	//		stack.Build.Add(
	//			middleware.BuildMiddlewareFunc("",
	//			func(ctx context.Context, input middleware.BuildInput, handler middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
	//			parsedURL, err := url.Parse(input.Request.URL.String())
	//			if err != nil {
	//				log.Fatal("Failed to parse URL", err)
	//			}
	//
	//			input.Request.URL.Opaque = parsedURL.Path
	//
	//			return handler.HandleBuild(ctx, input)
	//		},
	//		), middleware.After)
	//
	//		// Use the built-in Sign middleware
	//		stack.Sign.Add(middleware.SignRequestHandlerV4)
	//
	//		// Use the built-in ContentLength middleware
	//		stack.Sign.AddRequestHandler(middleware.BuildContentLengthHandler)
	//
	//		stack.Build.Add(middleware.BuildMiddlewareFunc(), middleware.After)
	//		corehandlers.UserAgentHandler)
	//		svc.Handlers.Build.PushBack(func(r *request.Request) {
	//			parsedURL, err := url.Parse(r.HTTPRequest.URL.String())
	//			if err != nil {
	//				log.Fatal("Failed to parse URL", err)
	//			}
	//			r.HTTPRequest.URL.Opaque = parsedURL.Path
	//		})
	//
	//		svc.Handlers.Sign.Clear()
	//		svc.Handlers.Sign.PushBack(Sign)
	//		svc.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
	//
	//	})
	//})
	//
	//s3Client.GetObject()
	//usev2, ok := config.Config(ConfigV2Signing)
	//if ok && usev2 == "true" {
	//	setv2Handlers(s3Client)
	//}

	return s3Client, endpoint, nil
}
