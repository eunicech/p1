package main

import (
	"container/list"
	"fmt"
)

func main() {
	fmt.Println("Go Linked Lists Tutorial")

	mylist := list.New()
	mylist.PushBack(1)
	mylist.PushBack(2)
	mylist.PushBack(3)
	mylist.PushBack(4)
	mylist.PushBack(5)

	currElem := mylist.Front()
	for currElem != nil {
		curr := currElem.Value.(int)
		fmt.Printf("Current num: %d\n", curr)
		next := currElem.Next()
		if curr == 2 || curr == 4 {
			mylist.Remove(currElem)
		}
		currElem = next

	}
	for element := mylist.Front(); element != nil; element = element.Next() {
		fmt.Println(element.Value)
	}
}
