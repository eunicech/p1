package main

import (
	"container/list"
	"fmt"
	"time"
)

type check struct {
	mylist *list.List
}

func main() {
	fmt.Println("Go Linked Lists Tutorial")
	chk := &check{
		mylist: list.New(),
	}
	chk.mylist.PushBack(1)
	chk.mylist.PushBack(2)
	chk.mylist.PushBack(3)
	chk.mylist.PushBack(4)
	chk.mylist.PushBack(5)

	currElem := chk.mylist.Front()
	for currElem != nil {
		curr := currElem.Value.(int)
		fmt.Printf("Current num: %d\n", curr)
		next := currElem.Next()
		if curr == 2 || curr == 4 {
			chk.mylist.Remove(currElem)
		}
		currElem = next

	}
	for element := chk.mylist.Front(); element != nil; element = element.Next() {
		fmt.Println(element.Value)
	}
	start := time.Date(2009, 1, 1, 12, 0, 0, 0, time.UTC)
	afterTenSeconds := start.Add(time.Second * 10)
	afterTenMinutes := start.Add(time.Minute * 10)
	afterTenHours := start.Add(time.Hour * 10)
	afterTenDays := start.Add(time.Hour * 24 * 10)

	fmt.Printf("start = %v\n", start)
	fmt.Printf("start.Add(time.Second * 10) = %v\n", afterTenSeconds)
	fmt.Printf("start.Add(time.Minute * 10) = %v\n", afterTenMinutes)
	fmt.Printf("start.Add(time.Hour * 10) = %v\n", afterTenHours)
	fmt.Printf("start.Add(time.Hour * 24 * 10) = %v\n", afterTenDays)
}
