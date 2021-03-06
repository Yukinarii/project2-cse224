package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"strconv"
	"math"
	"net"
	"sort"
	"bytes"
	"io/ioutil"
	"io"
	"time"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type records struct {
	serverRecords [][]byte
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func getServerId(firstN int, data []byte) int {
	nb := 0
	id := 0

	exit:
	for _, d := range data {
		num := int(d)
		for i := 7; i >= 0; i-- {
			if nb >= firstN {
				break exit
			}
			id = id << 1 + (num & (1 << i) >> i)
			nb++
		}
	}
	return id
}

func getAddr(scs ServerConfigs, serverId int) (host string, port string){
	for i := range scs.Servers {
		if scs.Servers[i].ServerId == serverId {
			host = scs.Servers[i].Host
			port = scs.Servers[i].Port
		}
	}
	return host, port
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	// variables
	input := os.Args[2]
	output := os.Args[3]
	num_server := len(scs.Servers)
	firstN := int(math.Log2(float64(num_server)))
	var localRecords [][]byte

	// read file and arrange by serverId
	data, err := ioutil.ReadFile(input)
	check(err)

	r := make([]records, num_server)
	for i := 0; i < len(data) / 100; i++ {
		sId := getServerId(firstN, data[i * 100: i * 100 + 10])
		r[sId].serverRecords = append(r[sId].serverRecords, data[i * 100: (i + 1) * 100])
	}

	for i:= 0; i < num_server; i++ {
		fmt.Println(len(r[i].serverRecords), " ")
	}
	// communication stage
	host, port := getAddr(scs, serverId)

	done := make(chan []byte)


	listener, err := net.Listen("tcp", host + ":" + port)
	if err != nil {
		log.Printf("listener build failed")
		os.Exit(1)
	}

	for i := range scs.Servers {
		if scs.Servers[i].ServerId == serverId {
			continue
		}

		// connect to the other servers and receive records from them
		go func(c chan []byte, id int, host string, port string) {
			var conn net.Conn
			var err error
			var res []byte

			for {
				conn, err = net.Dial("tcp",  host+ ":" + port)
				if err != nil {
					time.Sleep(time.Millisecond * 10)
				} else {
					fmt.Println("connect to ", host)
					break
				}
			}
			defer conn.Close()

			// send id to tell server the interested data
			_, err = conn.Write([]byte(strconv.Itoa(serverId)))
			check(err)
			buf := make([]byte, 100)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					if err != io.EOF {
						log.Printf("Received data failed %v", err)
						continue
					} else {
						fmt.Println("All data has been received")
						break
					}
				}
				
				if string(buf[:n]) == "Finished" {
					fmt.Println("All data has been received")
					break
				}
				//fmt.Println("received ", buf, " from" , id)
				res = append(res, buf[:n]...)
			}
			fmt.Println(len(res), "records are received from ", host)
			c <- res
		}(done, scs.Servers[i].ServerId, scs.Servers[i].Host, scs.Servers[i].Port)
	}

	// send records out
	nc := 0
	sc := make(chan bool)
	for {
		if nc == num_server - 1 {
			break
		}
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("server connecetion failed")
			continue
		}

		nc++
		go func(conn net.Conn, c chan bool) {
			defer conn.Close()

			buf := make([]byte, 16)
			n, err := conn.Read(buf)
			if err != nil {
				log.Printf("received serverId failed")
			}
			sid, _ := strconv.Atoi(string(buf[:n]))

			cnt := 0
			for i:= 0; i < len(r[sid].serverRecords); i++ {
				payload := r[sid].serverRecords[i]
				_, err = conn.Write(payload)
				if err != nil {
					log.Printf("sent failed")
				}
				cnt++
				//fmt.Println("send to ", sid)
			}

			conn.Write([]byte("Finished"))
			fmt.Println("accept a connection from client ", sid, "and sent ", cnt, " records")
			c <- true
		}(conn, sc)
	}

	numClientsComplete := 0
	for {
		if numClientsComplete == num_server - 1 {
			break
		}
		data := <-done
		numClientsComplete += 1
		for i := 0; i < len(data) / 100; i++ {
			localRecords = append(localRecords, data[i * 100: (i + 1) * 100])
			//fmt.Println("appended ", data[i * 100: (i + 1) * 100])
		}
		fmt.Println(len(data)/100, "records are appended")
	}
	//fmt.Println("communication stage ends")

	for i := 0; i < len(r[serverId].serverRecords); i++ {
		localRecords = append(localRecords, r[serverId].serverRecords[i])
	}

	for i := 0; i < num_server - 1; i++ {
		<- sc
	}
	//fmt.Println("sorting")
	sort.Slice(localRecords, func(i, j int) bool {
		return bytes.Compare(localRecords[i][:10], localRecords[j][:10]) < 0
	})

	fmt.Println("output")
	ioutil.WriteFile(output, bytes.Join(localRecords, []byte("")), 0666)

}
