package net

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/maps"
)

var (
	testdataFolder = "../../testdata/rule"
	tccTestServer  = ":6335"
	// testConfigServer = "127.0.0.1:8889"
	msgContent1 = "{\"test\":\"AA\"}"
	msgContent2 = "{\"test\":\"BB\"}"
	msgContent3 = "\"test\":\"CC\\n aa\""
	msgContent4 = "{\"test\":\"DD\"}"
	msgContent5 = "{\"test\":\"FF\"}"
	tcpConnCfg  = &Config{
		Protocol:    "tcp",
		Server:      tccTestServer,
		ReadTimeout: 1,
	}
	udpConnCfg = &Config{
		Protocol:    "udp",
		Server:      "127.0.0.1:6336",
		ReadTimeout: 1,
	}
)

// 测试请求/响应消息
func TestNetMessage(t *testing.T) {
	t.Run("Request", func(t *testing.T) {
		var request = &RequestMessage{}
		test.EndpointMessage(t, request)
	})
	t.Run("Response", func(t *testing.T) {
		var response = &ResponseMessage{}
		test.EndpointMessage(t, response)
	})
}

func TestRouterId(t *testing.T) {

	for _, cfg := range [2]*Config{tcpConnCfg, udpConnCfg} {
		t.Run(cfg.Protocol, func(t *testing.T) {
			config := types.NewConfig()

			var nodeConfig = make(types.Configuration)
			_ = maps.Map2Struct(cfg, nodeConfig)
			var ep = &Net{}
			err := ep.Init(config, nodeConfig)
			assert.Nil(t, err)
			router := impl.NewRouter().SetId("r1").From("/device/info").End()
			routerId, _ := ep.AddRouter(router)
			assert.Equal(t, "r1", routerId)

			router = impl.NewRouter().From("/device/info").End()
			routerId, _ = ep.AddRouter(router)
			assert.Equal(t, "/device/info", routerId)
			router = impl.NewRouter().From("/device/info").End()
			routerId, _ = ep.AddRouter(router, "test")
			assert.Equal(t, "/device/info", routerId)

			err = ep.RemoveRouter("r1")
			assert.Nil(t, err)
			err = ep.RemoveRouter("/device/info")
			assert.Nil(t, err)
			err = ep.RemoveRouter("/device/info")
			assert.Equal(t, fmt.Sprintf("router: %s not found", "/device/info"), err.Error())
		})
	}

}

func TestNetEndpoint(t *testing.T) {
	var wg sync.WaitGroup

	for _, cfg := range []*Config{udpConnCfg, tcpConnCfg} {

		//发送消息
		metaData := types.BuildMetadata(make(map[string]string))
		stop := make(chan struct{})
		wg.Add(1)
		// 启动服务
		go func() {
			startServer(t, cfg, stop, &wg)
		}()
		time.Sleep(time.Millisecond * 200)

		t.Run(cfg.Protocol, func(t *testing.T) {
			node := createNetClient(t, cfg)
			config := types.NewConfig()
			ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
				assert.Equal(t, types.Success, relationType)
			})
			msg1 := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, msgContent1)
			node.OnMsg(ctx, msg1)
			msg2 := ctx.NewMsg("TEST_MSG_TYPE_BB", metaData, msgContent2)
			node.OnMsg(ctx, msg2)
			msg3 := ctx.NewMsg("TEST_MSG_TYPE_CC", metaData, msgContent3)
			node.OnMsg(ctx, msg3)
			//因为服务器与\n或者\t\n分割，这里会收到2条消息
			msg4 := ctx.NewMsg("TEST_MSG_TYPE_DD", metaData, msgContent4+"\n"+msgContent5)
			node.OnMsg(ctx, msg4)
			//ping消息
			msg5 := ctx.NewMsg(PingData, metaData, PingData)
			node.OnMsg(ctx, msg5)
			//等待发送心跳
			time.Sleep(time.Second * 1)
			//销毁并 断开连接
			node.Destroy()

			//停止服务器
			stop <- struct{}{}
		})
	}
	wg.Wait()

}

func TestNetEndpointConfig(t *testing.T) {
	for _, cfg := range [2]*Config{tcpConnCfg, udpConnCfg} {
		t.Run(cfg.Protocol, func(t *testing.T) {
			config := engine.NewConfig(types.WithDefaultPool())
			//创建tpc endpoint服务
			var nodeConfig = make(types.Configuration)
			_ = maps.Map2Struct(cfg, nodeConfig)
			var epStarted = &Net{}
			err := epStarted.Init(config, nodeConfig)
			assert.Nil(t, err)

			assert.Equal(t, cfg.Server, epStarted.Id())

			err = epStarted.Start()
			assert.Nil(t, err)

			time.Sleep(time.Millisecond * 200)

			nodeConfig = make(types.Configuration)
			_ = maps.Map2Struct(&Config{
				Server:   cfg.Server,
				Protocol: cfg.Protocol,
				//1秒超时
				ReadTimeout: 1,
			}, nodeConfig)
			var netEndpoint = &Net{}
			err = netEndpoint.Init(config, nodeConfig)
			assert.Nil(t, err)
			assert.Equal(t, cfg.Protocol, netEndpoint.Config.Protocol)

			//启动失败，端口已经占用
			err = netEndpoint.Start()
			assert.NotNil(t, err)

			netEndpoint = &Net{}

			err = netEndpoint.Init(types.NewConfig(), types.Configuration{
				"server":   cfg.Server,
				"protocol": cfg.Protocol,
				//1秒超时
				"readTimeout": 1,
			})
			assert.Nil(t, err)

			assert.Equal(t, cfg.Protocol, netEndpoint.Config.Protocol)

			var ep = &Net{}
			err = ep.Init(config, nodeConfig)
			assert.Nil(t, err)
			assert.Equal(t, cfg.Server, ep.Id())
			_, err = ep.AddRouter(nil)
			assert.Equal(t, "router can not nil", err.Error())

			router := impl.NewRouter().From("^{.*").End()
			routerId, err := ep.AddRouter(router)
			assert.Nil(t, err)

			//重复
			router = impl.NewRouter().From("^{.*").End()
			_, err = ep.AddRouter(router)
			assert.Equal(t, "duplicate router ^{.*", err.Error())

			//删除路由
			_ = ep.RemoveRouter(routerId)

			router = impl.NewRouter().From("^{.*").End()
			_, err = ep.AddRouter(router)
			assert.Nil(t, err)

			//错误的表达式
			router = impl.NewRouter().From("[a-z{1,5}").End()
			_, err = ep.AddRouter(router)
			assert.NotNil(t, err)

			epStarted.Destroy()
			netEndpoint.Destroy()
		})

	}
}
func createNetClient(t *testing.T, cfg *Config) types.Node {
	node, _ := engine.Registry.NewNode("net")
	var configuration = make(types.Configuration)
	configuration["protocol"] = cfg.Protocol
	configuration["server"] = cfg.Server

	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func startServer(t *testing.T, connConfig *Config, stop chan struct{}, wg *sync.WaitGroup) {
	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Error(err)
	}
	config := engine.NewConfig(types.WithDefaultPool())
	//注册规则链
	_, _ = engine.New("default", buf, engine.WithConfig(config))

	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(connConfig, nodeConfig)

	var ep = &Net{}
	err = ep.Init(config, nodeConfig)
	assert.Equal(t, err, nil)
	assert.Equal(t, Type, ep.Type())
	assert.True(t, reflect.DeepEqual(&Net{
		Config: *connConfig,
	}, ep.New()))

	//添加全局拦截器
	ep.AddInterceptors(func(router endpoint.Router, exchange *endpoint.Exchange) bool {
		//权限校验逻辑
		return true
	})
	var router1Count = int32(0)
	var router2Count = int32(0)
	//匹配所有消息，转发到该路由处理

	router1 := impl.NewRouter().From("").Transform(func(router endpoint.Router, exchange *endpoint.Exchange) bool {
		defer func() {
			l := recover()
			if l != nil {
				t.Log("recover:", l)
			}
		}()
		from := exchange.In.From()
		requestMessage, ok := exchange.In.(*RequestMessage)
		assert.True(t, ok)
		assert.True(t, requestMessage.Conn() != nil)
		assert.Equal(t, from, requestMessage.From())

		exchange.In.GetMsg().Type = "TEST_MSG_TYPE2"
		receiveData := exchange.In.GetMsg().Data
		fmt.Println("data:", string(receiveData))

		if receiveData != msgContent1 && receiveData != msgContent2 && receiveData != msgContent3 && receiveData != msgContent4 && receiveData != msgContent5 {
			t.Fatalf("receive data:%s,expect data:%s,%s,%s,%s,%s", receiveData, msgContent1, msgContent2, msgContent3, msgContent4, msgContent5)
		}

		assert.True(t, strings.Contains(from, "127.0.0.1"))
		assert.Equal(t, from, exchange.In.Headers().Get(RemoteAddrKey))
		assert.Equal(t, exchange.In.Headers().Get(RemoteAddrKey), exchange.In.GetMsg().Metadata.GetValue(RemoteAddrKey))

		atomic.AddInt32(&router1Count, 1)
		return true
	}).To("chain:default").
		Process(func(router endpoint.Router, exchange *endpoint.Exchange) bool {
			assert.Equal(t, exchange.Out.From(), exchange.Out.Headers().Get(RemoteAddrKey))
			v := exchange.Out.GetMsg().Metadata.GetValue("addFrom")
			assert.True(t, v != "")
			//发送响应
			exchange.Out.SetBody([]byte("response"))
			return true
		}).End()

	//匹配与{开头的消息，转发到该路由处理
	router2 := impl.NewRouter().From("^{.*").Transform(func(router endpoint.Router, exchange *endpoint.Exchange) bool {
		defer func() {
			l := recover()
			if l != nil {
				t.Log("recover:", l)
			}
		}()
		exchange.In.GetMsg().Type = "TEST_MSG_TYPE2"
		receiveData := exchange.In.GetMsg().Data
		if strings.HasSuffix(receiveData, "{") {
			t.Errorf("receive data:%s,not match data:%s", receiveData, "^{.*")
		}
		atomic.AddInt32(&router2Count, 1)
		return true
	}).To("chain:default").End()

	//注册路由
	_, err = ep.AddRouter(router1)
	if err != nil {
		t.Error(err)
	}
	_, err = ep.AddRouter(router2)
	if err != nil {
		t.Error(err)
	}
	//启动服务
	err = ep.Start()

	assert.Nil(t, err)
	<-stop
	assert.Equal(t, int32(5), router1Count)
	assert.Equal(t, int32(4), router2Count)
	wg.Done()

}
