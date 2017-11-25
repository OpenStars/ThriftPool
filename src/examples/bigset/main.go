package main

//Server C++ can get at github.com/openstars/binary/bigset

import (
//	"thriftpool"
	bs "openstars/core/bigset/generic"
	"git.apache.org/thrift.git/lib/go/thrift"
	"time"
	"fmt"
	"thriftpool"

	"strconv"
)

//type ThriftCreator func(ip, port string, connTimeout time.Duration, forPool* ThriftPool) (*ThriftSocketClient, error)

//type ThriftClientClose func(c *ThriftSocketClient) error

func BigSetClientCreator(host, port string, connTimeout time.Duration, forPool* thriftpool.ThriftPool) (*thriftpool.ThriftSocketClient, error){
	socket, err := thrift.NewTSocketTimeout(fmt.Sprintf("%s:%s", host, port), connTimeout)
	if err != nil {
		return nil, err
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := bs.NewTStringBigSetKVServiceClientFactory(transportFactory.GetTransport(socket), protocolFactory)

	err = client.Transport.Open()
	if err != nil {
		return nil, err
	}

	return &thriftpool.ThriftSocketClient{
		Client: client,
		Socket: socket,
		Parent: forPool,
	}, nil
}

func Close(c *thriftpool.ThriftSocketClient) error {
	err := c.Socket.Close()
	//err = c.Client.(*tutorial.PlusServiceClient).Transport.Close()
	return err
}

//GlobalRpcPool = thriftPool.NewThriftPool("10.5.20.3", "23455", 100, 32, 600, Dial, Close)

var (mp=thriftpool.NewMapPool(100, 3600, 3600, BigSetClientCreator, Close))

func testPoolWrite(num int){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	if (client == nil ) {
		return
	}
	defer client.BackToPool();
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("testBigSet", &bs.TItem{[]byte("key_" + strconv.Itoa(num)), []byte("Value_"+ strconv.Itoa(num))  } )

	fmt.Println(mp.Get("127.0.0.1", "18407").GetConnCount(), mp.Get("127.0.0.1", "18407").GetIdleCount() )
}

func testPoolRead(num int){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	if (client == nil ){
		return;
	}

	defer client.BackToPool();
	//client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("testBigSet", &bs.TItem{[]byte("key_" + strconv.Itoa(num)), []byte("Value_"+ strconv.Itoa(num))  } )
	//res, _:=client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("myBigSet", []byte("Hello"))
	//fmt.Println((string)(res.Item.Value[:]) )
	res, _:=client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("testBigSet", []byte("key_" + strconv.Itoa(num)) )
	if (res !=nil && res.Item != nil && res.Item.Value != nil) {
		fmt.Println((string)(res.Item.Value[:]) )
	}


}

func main(){
	//mp:=thriftpool.NewMapPool(100, 3600, 3600, BigSetClientCreator, Close)

	client, _ := mp.Get("127.0.0.1", "18407").Get()

	defer client.BackToPool();
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("myBigSet", &bs.TItem{[]byte("Hello"),[]byte("Xin chao")})
	res, _:=client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("myBigSet", []byte("Hello"))
	fmt.Println((string)(res.Item.Value[:]) )

	for i:=0; i<1000; i++ {
		go testPoolWrite(i);
	}

	time.Sleep(2000);
	time.Sleep(time.Second * 1)
	//push client back to pool
	for i:=100; i<200; i++ {
		go testPoolRead(i);
	}
	time.Sleep(time.Second * 5)

}
