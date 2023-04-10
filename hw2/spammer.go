package main

import (
	"log"
	"sort"
	"strconv"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})

	for _, c := range cmds {
		wg.Add(1)
		out := make(chan interface{})

		go func(c cmd, in, out chan interface{}) {
			defer wg.Done()
			c(in, out)
			close(out)
		}(c, in, out)

		in = out
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	set := make(map[string]bool)
	setMutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	for val := range in {
		email := val.(string)
		wg.Add(1)
		go func() {
			defer wg.Done()
			user := GetUser(email)
			setMutex.Lock()
			defer setMutex.Unlock()
			if _, ok := set[user.Email]; !ok {
				set[user.Email] = true
				out <- user
			}
		}()
	}
	wg.Wait()
}

func getMsgWorker(butchUsr []User, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	messages, err := GetMessages(butchUsr...)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range messages {
		out <- msg
	}
}

func SelectMessages(in, out chan interface{}) {
	butchUsr := make([]User, 0, GetMessagesMaxUsersBatch)
	wg := sync.WaitGroup{}
	for val := range in {
		usr := val.(User)
		butchUsr = append(butchUsr, usr)
		if len(butchUsr) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			res := make([]User, len(butchUsr))
			copy(res, butchUsr)
			go getMsgWorker(res, out, &wg)
			butchUsr = butchUsr[:0]
		}
	}
	if len(butchUsr) > 0 {
		wg.Add(1)
		go getMsgWorker(butchUsr, out, &wg)
	}
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	var sem = make(chan struct{}, HasSpamMaxAsyncRequests)
	for val := range in {
		msgId := val.(MsgID)
		sem <- struct{}{}
		wg.Add(1)

		go func() {
			defer wg.Done()

			has, err := HasSpam(msgId)
			<-sem
			if err != nil {
				log.Fatal(err)
			}

			out <- MsgData{
				ID:      msgId,
				HasSpam: has,
			}
		}()
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var data []MsgData
	for val := range in {
		data = append(data, val.(MsgData))
	}
	sort.Slice(data, func(i, j int) bool {
		if data[i].HasSpam == data[j].HasSpam {
			return data[i].ID < data[j].ID
		}
		return data[i].HasSpam && !data[j].HasSpam
	})

	for _, msg := range data {
		out <- strconv.FormatBool(msg.HasSpam) + " " + strconv.FormatUint(uint64(msg.ID), 10)
	}
}
