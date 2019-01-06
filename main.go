package main

import "fmt"
import "time"
import "math/rand"

func RandomsInt(n int) (r []int) {
    rnd := rand.New(rand.NewSource(99))
    r = make([]int, n)
    for i, _ := range r {
        r[i] = rnd.Int()
    }
    return
}

func Min(a, b int) int {
    switch {
    case a > b:
        return b
    default:
        return a
    }
}

// ========= Channel version

// this function feeds the SumPLL function in chunks of appropriate size < 8 MB L3 cache
func ChanSumPLL(nums []int, num_workers, chunk_size int) (sum int) {

    // channels
    c := make(chan []int)
    done := make(chan struct{})

    // start consumer
    go func(){  
        for true {
            if slice, ok := <-c; ok { // blocks until channel is available or closed 
                sum += SumPLL(slice[:], num_workers) // delegates to workers
            } else {
                break
            }
        }
        done <- struct{}{}
    }()

    // feed consumer
    for n := 0; true; n++ {
        start, stop := n*chunk_size, Min((n+1)*chunk_size, len(nums))
        c <- nums[start : stop] // feed slice to consumer
        if (n+1)*chunk_size >= len(nums) { // no more slices
            close(c)  // tell consumer
            break
        }
    }

    // wait for consumer to finish
    <- done

    return
}

// =========

func Sum(nums []int) (sum int) {
    for i := 0; i < 250; i++ {
    for _, n := range nums {
        sum += n
    }
}   
    return
}

func SumPLL(nums []int, num_workers int) (sum int) {

    // safety
    if num_workers < 1 {
        panic("need at least one worker!!!")
    }

    // convenience
    L := len(nums) / (num_workers -1)

    // divide into slices
    slices := make([][]int, num_workers)
    for n := range slices {
        slices[n] = nums[n*L : Min((n+1)*L, len(nums))]
    }

    // dispatch work
    //start := time.Now()
    results := make(chan int, 1)
    for n := range slices {
        s := slices[n]
        go func() {
            results <- Sum(s[:])
        }()
    }

    // aggregate work
    for n := 0; n < num_workers; n++ {
        sum += <-results
        //fmt.Printf("%d : %s\n", n, time.Now().Sub(start))
    }

    return
}

func benchSum(randoms []int) time.Duration {
    start := time.Now()
    res := Sum(randoms[:])
    end := time.Now()

    fmt.Println("sum : ", res)
    return end.Sub(start)
}

func benchSumPLL(randoms []int, N int) time.Duration {
    start := time.Now()
    res := SumPLL(randoms[:], N)
    end := time.Now()

    fmt.Println("sum : ", res)
    return end.Sub(start)
}

func benchChanSumPLL(randoms []int, num_workers, chunk_size int) time.Duration {

    start := time.Now()
    res := ChanSumPLL(randoms[:], num_workers, chunk_size)
    end := time.Now()

    fmt.Println("sum : ", res)
    return end.Sub(start)
}

func horline() {
    for i := 0; i < 40; i++ {
        fmt.Printf("=")
    }
    fmt.Println()
}

func speedRatio(a, b time.Duration) {
    fmt.Printf("speed improvement factor : %f\n", a.Seconds() / b.Seconds())
}

func main() {

    chunk_size := 20*30000
    randoms := RandomsInt(300*30000)
    fmt.Println("done making randoms")

    horline()
    dur1 := benchSum(randoms[:])
    fmt.Printf("Sum : %s elapsed \n", dur1)
    speedRatio(dur1, dur1)

    horline()
    dur2 := benchSumPLL(randoms[:], 7)
    fmt.Printf("SumPLL : %s elapsed \n", dur2)
    speedRatio(dur1, dur2)

    avg3 := 0.0
    N := 10
    for i := 0; i < N; i ++ { 
        horline()
        dur3 := benchChanSumPLL(randoms[:], 7, chunk_size)
        avg3 += dur3.Seconds()
        fmt.Printf("ChanSumPLL : %s elapsed \n", dur3)
        speedRatio(dur1, dur3)
    }
    avg3 /= float64(N)
    fmt.Printf("average improvement factor : %f\n", dur1.Seconds()/avg3)

}                     
