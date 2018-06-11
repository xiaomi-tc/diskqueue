package main

import (
	"os"
	"os/signal"
	log "github.com/xiaomi-tc/log15"
	gdq "github.com/xiaomi-tc/diskqueue"
	"fmt"
	"time"
	"strconv"
)

var c chan os.Signal
var cleanupDone chan bool

func waitToExit() {
	c =make(chan os.Signal,1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func() {
		for _= range c {
			log.Info("get signal:")
			cleanupDone <- true
		}
	}()
}

func create(dq gdq.Interface) {
	log.Info("","len:",dq.Depth())
	basestring := "testasdfasdfas--"
	start := time.Now()
	for i :=0; i<1000000; i++ {

		msg := []byte(basestring + strconv.Itoa(i))
		if err := dq.Put(msg); err != nil {
			fmt.Println(err)
			return
		}

		if i%10000 ==0 {
			log.Info("","Put:", string(msg),"i:",i)
		}
	}

	log.Info("","end put, total:", dq.Depth(), " use time:", time.Since(start))

	return
}

func consum(dq gdq.Interface) {
	count :=0
	for {

		length := dq.Depth()
		for i := int64(0); i<length; i++{
			msgOut := <-dq.ReadChan()
			count++
			if count%10000 ==0 {
				log.Info("    ", "Get:", string(msgOut), "count:",count)
			}
		}

		time.Sleep(time.Millisecond *50)
	}

	return
}
func main() {

	dqName := "mq"
	tmpDir := "./data"
	// create dir if it does not exist
	if _, err := os.Stat(tmpDir); err != nil {

		log.Info("path " + tmpDir + " not exists " )
		err := os.MkdirAll(tmpDir, os.ModePerm)

		if err != nil {
			log.Info("Error creating directory")
			log.Info(err.Error())
			return
		}
	}

	dq := gdq.New(dqName,
		tmpDir,
		1024*1024*4,
		4,
		1<<10,
		2500, 2*time.Second,
		)
	if dq == nil {
		log.Info("dq open failed")
		return
	}
	defer dq.Close()

	go create(dq)
	go consum(dq)

	cleanupDone = make(chan bool)
	waitToExit()
	<-cleanupDone
}