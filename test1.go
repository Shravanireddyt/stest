package database

import (
	"fmt"
	"sync"

	pf "Project1/bookService"

	jd "Project1"

	"github.com/couchbase/gocb/v2"
)

var CollectionH syncedcbBucketHandle

//go get github.com/couchbase/gocb/v2@v2.3.0

type syncedcbBucketHandle struct {
	bucketLock       sync.Mutex
	scopeHandler     *gocb.Scope
	CollectionHandle *gocb.Collection
}

const (
	BUCKET_NAME     string = "BookReviewSys"
	SCOPE_NAME      string = "bookdata"
	COLLECTION_NAME string = "BookInfo"
)

func CBInitialize() {

	cluster, err := gocb.Connect(
		"localhost",
		gocb.ClusterOptions{
			Username: "Administrator",
			Password: "Administrator",
		})
	if err != nil {
		panic(err)
	}

	bucketMgr := cluster.Buckets()
	if _, err := bucketMgr.GetBucket(BUCKET_NAME, nil); err != nil {
		err = bucketMgr.CreateBucket(gocb.CreateBucketSettings{
			BucketSettings: gocb.BucketSettings{
				Name:                 BUCKET_NAME,
				FlushEnabled:         false,
				ReplicaIndexDisabled: true,
				RAMQuotaMB:           150,
				NumReplicas:          0,
				BucketType:           gocb.CouchbaseBucketType,
			},
			ConflictResolutionType: gocb.ConflictResolutionTypeSequenceNumber,
		}, nil)
		if err != nil {
			panic(err)
		}
	}

	bucket := cluster.Bucket(BUCKET_NAME)

	bucketmgr := bucket.Collections()
	AllScopes, _ := bucketmgr.GetAllScopes(nil)
	var test bool = false
	for _, v := range AllScopes {
		if v.Name == SCOPE_NAME {
			test = true
		}
	}
	if test != true {
		if err := bucketmgr.CreateScope(SCOPE_NAME, nil); err != nil {
			panic(err)
		}
	}

	if AllScopes[0].Collections[0].Name != COLLECTION_NAME {
		if err := bucketmgr.CreateCollection(
			gocb.CollectionSpec{
				Name:      COLLECTION_NAME,
				ScopeName: SCOPE_NAME,
			}, nil); err != nil {
			panic(err)
		}
		query := fmt.Sprintf("create primary index on `%s`.%s.%s ", BUCKET_NAME, SCOPE_NAME, COLLECTION_NAME)
		_, err := bucket.Scope(SCOPE_NAME).Query(query, nil)
		if err != nil {
			panic("can't create index")
		}
	}

	CollectionH.scopeHandler = bucket.Scope(SCOPE_NAME)
	CollectionH.CollectionHandle = CollectionH.scopeHandler.Collection(COLLECTION_NAME)

}

func GetBookFromDB(lt int32) ([]pf.Book, bool) {

	fmt.Println("In getbook couchbase")

	CollectionH.bucketLock.Lock()

	var books []pf.Book

	query1 := fmt.Sprintf("select name,id,author,shortdesc from `%s`.%s.%s limit $1", BUCKET_NAME, SCOPE_NAME, COLLECTION_NAME)
	fmt.Println(query1)
	rows, err := CollectionH.scopeHandler.Query(query1, &gocb.QueryOptions{PositionalParameters: []interface{}{lt}})
	CollectionH.bucketLock.Unlock()
	if err != nil {
		fmt.Println("error found")
		return books, true
	}

	fmt.Println(rows)

	for rows.Next() {
		var bk pf.Book
		err := rows.Row(&bk)
		if err != nil {
			return books, true
		}
		books = append(books, bk)
	}

	return books, false
}

func AddBookToDB(bk jd.BookReview) bool {

	fmt.Println("In addbook couchbase")
	CollectionH.bucketLock.Lock()
	_, err := CollectionH.CollectionHandle.Insert(fmt.Sprintf("%d", bk.ID), &bk, nil)
	CollectionH.bucketLock.Unlock()
	if err != nil {
		return true
	}
	return false
}

func AddReviewToDB(id uint32, rv jd.Review) {

	fmt.Println("In addreview couchbase")
	mops := []gocb.MutateInSpec{
		gocb.ArrayAppendSpec("reviews", rv, nil),
	}

	query := fmt.Sprintf("select reviews from `%s`.%s.%s  where id=$1", BUCKET_NAME, SCOPE_NAME, COLLECTION_NAME)
	fmt.Println(query)
	CollectionH.bucketLock.Lock()
	insertResult, err := CollectionH.CollectionHandle.MutateIn(fmt.Sprintf("%d", id), mops, &gocb.MutateInOptions{})
	CollectionH.bucketLock.Unlock()
	if err != nil {
		panic(err)
	}
	fmt.Println(insertResult)

}

func GetReviewFromDB(id uint32) ([]pf.Review, bool) {

	fmt.Println("In get review couchbase")
	query1 := fmt.Sprintf("select reviews from `%s`.%s.%s where id=$1", BUCKET_NAME, SCOPE_NAME, COLLECTION_NAME)
	CollectionH.bucketLock.Lock()
	rows, err := CollectionH.scopeHandler.Query(query1, &gocb.QueryOptions{PositionalParameters: []interface{}{id}})
	CollectionH.bucketLock.Unlock()
	if err != nil {
		fmt.Println("error found")
		panic(err)
	}
	var rv jd.ReviewArr2
	for rows.Next() {
		err := rows.Row(&rv)
		if err != nil {
			panic(err)
		}
	}
	var reviews []pf.Review
	for _, r := range rv.Reviews {
		reviews = append(reviews, pf.Review{Name: r.Name, Score: r.Score, Text: r.Text})
	}
	return reviews, false
}
