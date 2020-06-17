#include "threadpool.h"
#include "list.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <semaphore.h>
#include <errno.h>
/**********
    resources used: 
    https://docs.oracle.com/cd/E19253-01/816-5137/6mba5vqmn/index.html
    https://nachtimwald.com/2019/04/12/thread-pool-in-c/
**********/

/**
 * This is the struct that defines the threadpool. It is used to store the worker threads and has
 * a list of variables to enable synchronization
 **/
typedef struct thread_pool
{
    //the global submission queue
    struct list taskQueue;
    //worker threads in the pool
    struct list workerThreads;
    //need flag to denote when worker is shutting down
    volatile bool shutDown;
    //need number of threads
    int numOfThreads;
    //need the mutex
    pthread_mutex_t poolMutex;
    //need condition variable to signal the worker threads that a new task is available
    pthread_cond_t newTaskAvailable;
    //Boolean variable that indicates that a new task is available
    volatile bool noNewWork;
    //Integer variable that keeps track of the number of tasks added to the pool
    volatile int numTasks;        
    //Semaphore that is used to indicate that all threads within pool have been initialized
    sem_t waitForAllThreads;
} thread_pool;

/* ENUM that defines the current status of a task */
typedef enum 
{
    BLOCKED = 0,
    IN_PROGRESS = 1,
    COMPLETE = 2
} taskStatus;

/* ENUM that defines the type of a task */
typedef enum
{
    INTERNAL = 0,
    EXTERNAL = 1
} taskType;

/**
 * This structure defines a task in the context of our program.
 **/
typedef struct future
{
    //need which function we're executing, doesnt matter what it is, just need to return it
    fork_join_task_t task;
    //need the args to pass into it
    void * args;
    //need the result from the function
    void * result;
    taskStatus status;
    //need the condition vars/ semaphores, since these are basically our jobs, sem would be best
    //sem_t sem;
    //need the element that contains the function
    struct list_elem elem;
    //0 = external task 1 = internal task
    int type;
    //Mutex and condition variables for a future
    pthread_mutex_t futureMutex;
    sem_t futureSem;
} future;


/* ENUM that defines the current status of a worker thread */
typedef enum 
{
    IDLE = 0,
    WORKING = 1
} workerStatus;

/**
 * Struct that defines a worker thread in the threadpool. Each worker thread has its own 
 * task queue and variables.
 **/
typedef struct worker
{
    //ThreadID of this worker thread
    pthread_t threadID;
    //Mutex and condition variables for the thread
    pthread_mutex_t tMutex;
    pthread_cond_t tCond;
    //Current status of the thread
    workerStatus status;
    //pointer to the thread pool this worker thread is in
    thread_pool *pool;
    //the task queue for this worker thread
    struct list taskQueue;
    //For use in list functionality
    struct list_elem elem;    
} worker;

/**
 * Helper function that retrieves the worker for the given thread ID from
 * the thread pool
 **/
static worker* getWorker(pthread_t tid, thread_pool *pool) {
    struct list_elem *currThread;
    for(currThread = list_begin(&pool->workerThreads); currThread != list_end(&pool->workerThreads); currThread =list_next(currThread)) {
        worker* toReturn = list_entry(currThread,struct worker, elem);
        if (toReturn->threadID == tid) {            
            return toReturn;
        }
    }
    return NULL;    
}

/**
 * Helper function that loops through the worker threads and steal the first task
 * available
 **/
static struct list_elem* stealTask(thread_pool *pool) {
    struct list_elem *currThread;
    struct list_elem* task = NULL;
    for(currThread = list_begin(&pool->workerThreads); currThread != list_end(&pool->workerThreads); currThread =list_next(currThread)) {
        worker* toReturn = list_entry(currThread,struct worker, elem);
        if (toReturn->threadID != pthread_self()) {
            pthread_mutex_lock(&toReturn->tMutex);        
            if (!list_empty(&toReturn->taskQueue)) {
                struct list_elem *currTask;
                //Check the last future in each queue to check for any avaialable tasks
                currTask = list_rbegin(&toReturn->taskQueue);
                future* taskToSteal = list_entry(currTask, struct future, elem);
                pthread_mutex_lock(&taskToSteal->futureMutex);
                if (taskToSteal->status == BLOCKED && taskToSteal->type == EXTERNAL) {
                    list_remove(currTask);                
                    pthread_mutex_unlock(&taskToSteal->futureMutex);
                    pthread_mutex_unlock(&toReturn->tMutex);
                    task = currTask;
                    break;
                }
                pthread_mutex_unlock(&taskToSteal->futureMutex);                        
            }
            pthread_mutex_unlock(&toReturn->tMutex);
        }     
    }
    return task;        
}

/**
 * This is the function that is run by pthread_create and is used to implement
 * a single work stealing thread within the threadpool
 * 
 * @param arg
 *      Points to the thread pool.
 **/
static void * implementThread(void * arg)
{
    thread_pool *pool = (thread_pool *)arg;
    //Wait for all threads to be initialized before starting implementation    
    sem_wait(&pool->waitForAllThreads);
    sem_post(&pool->waitForAllThreads);
    //Set cancel type to asynchronous so thread can be cancelled at any time
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    //Get thread ID for the thread implenting this function and find worker
    pthread_t myThreadID = pthread_self();
    worker* currWorker = getWorker(myThreadID, pool);
    volatile bool stop = false;
    //Thread will run while the thread pool is not shutdown
    while (!stop) {
        pthread_mutex_lock(&pool->poolMutex);
        if (pool->shutDown) {
            stop = true;            
            break;            
        }
        pthread_mutex_unlock(&pool->poolMutex);

        //First: Check workers queue for avaialable tasks
        //Second: If no tasks avaialble in the queue, check global queue
        //Third: Steal task from another worker
        struct list_elem* currTask;
        pthread_mutex_lock(&pool->poolMutex);
        pthread_mutex_lock(&currWorker->tMutex);
        if (list_empty(&currWorker->taskQueue)) {            
            if(list_empty(&pool->taskQueue)) {
                currTask = stealTask(pool);                
            }
            else {
                currTask = list_pop_front(&pool->taskQueue);
            }            
        }
        else {
            currTask = list_pop_front(&currWorker->taskQueue);
        }
        pthread_mutex_unlock(&currWorker->tMutex);
        pthread_mutex_unlock(&pool->poolMutex);

        //If no tasks are found put thread to sleep till signal
        if (currTask == NULL) {
            pthread_mutex_lock(&pool->poolMutex);            
            while (pool->noNewWork) {
                pthread_cond_wait(&pool->newTaskAvailable, &pool->poolMutex);
            }                   
            pthread_mutex_unlock(&pool->poolMutex);
        }
        else {
            //Once we have task we need to execute it
            future* taskToExec = list_entry(currTask, struct future, elem);
            pthread_mutex_lock(&taskToExec->futureMutex);
            taskToExec->status = IN_PROGRESS;
            taskToExec->result = taskToExec->task(pool, taskToExec->args);
            taskToExec->status = COMPLETE;
            pool->numTasks++;
            sem_post(&taskToExec->futureSem);            
            pthread_mutex_unlock(&taskToExec->futureMutex);
        }    
    }
    return NULL;
}

/** 
 * This function is used to create a new thread pool and initialize the needed structs for it to
 * function
 * 
 * @param nthreads
 *      The number of threads that need to be initiated within the threadpool
 **/
struct thread_pool * thread_pool_new(int nthreads)
{
    thread_pool * pool = (thread_pool *)malloc(sizeof(thread_pool));
    bool succesfullyCreated = true;
   	if (pool == NULL) 
    {
		printf("Error: Could not allocate memory needed to initialize thread pool\n");
		succesfullyCreated = false;
    }

    //Initialize threadpool variables
    list_init(&pool->taskQueue);
    list_init(&pool->workerThreads);
    pthread_mutex_init(&pool->poolMutex, NULL);
    pthread_cond_init(&pool->newTaskAvailable, NULL);
    pthread_mutex_lock(&pool->poolMutex);
    pool->shutDown = false;
    pool->numOfThreads = nthreads;
    pool->noNewWork = true;
    pool->numTasks = 0;
    sem_init(&pool->waitForAllThreads, 0, 1);
    sem_wait(&pool->waitForAllThreads);
    
    //Initialize the worker threads within the thread pool
    for (int i = 0; i < nthreads; i++) {
        struct worker* newThread = malloc(sizeof(worker));
        if (newThread == NULL) {
            printf("Could not allocate memory needed to initialize a worker thread within the pool");
            succesfullyCreated = false;
        }
        //Initialize the worker thread with the variables as follow
        else {
            list_init(&newThread->taskQueue);
            newThread->pool = pool;
            newThread->status = IDLE;
            pthread_mutex_init(&newThread->tMutex, NULL);
            pthread_cond_init(&newThread->tCond, NULL);
            if (pthread_create(&newThread->threadID, NULL, implementThread, pool) == 0) {
                //Add the worker thread to the threadpool
                list_push_front(&pool->workerThreads, &newThread->elem);                
            }
            else {
                printf("Error creating a new thread with pthread_create");
                succesfullyCreated = false;
            }               
        }        
    }

    pthread_mutex_unlock(&pool->poolMutex); 
     //If there is an error deallocate memory and destroy the threadpool
    if (!succesfullyCreated) {
        thread_pool_shutdown_and_destroy(pool);
    }
    sem_post(&pool->waitForAllThreads);   
    return pool;
}


/* 
 * Shutdown this thread pool in an orderly fashion.  
 * Tasks that have been submitted but not executed may or
 * may not be executed.
 *
 * Deallocate the thread pool object before returning. 
 * and terminate
 * can enter an inf loop waiting for tasks, want to eliminate busy waiting
 * inform threads that may or may not be blocked or checking vars
 * that they need to shut down (with the bool in the thread pool struct)
 *
 * says to check for jobs left in thread pool, they dont test this,
 * so threads, as soon as it is time to shut down, can disregard remaining tasks
 */
void thread_pool_shutdown_and_destroy(struct thread_pool *pool)
{
    if (pool == NULL) 
    {
        printf("The thread pool given in the arguement does not exist\n");        
    }
    else 
    {        
        pthread_mutex_lock(&pool->poolMutex);
        if (pool->shutDown) 
        {
            //Pool is already shut down by another thread
            return;
        }
        else 
        {
            pool->shutDown = true;
            pthread_cond_signal(&pool->newTaskAvailable);            
        }
        pthread_mutex_unlock(&pool->poolMutex);
    }
    //Eliminate all worker threads in the thread pool
    struct list_elem *currThread;
    for(currThread = list_begin(&pool->workerThreads); currThread != list_end(&pool->workerThreads); currThread =list_next(currThread)) 
    {
        worker *toEliminate = list_entry(currThread,struct worker, elem);
        pthread_cancel(toEliminate->threadID);
        pthread_mutex_destroy(&toEliminate->tMutex);
        pthread_cond_destroy(&toEliminate->tCond);        
        free(toEliminate);
    }    
    //Destroy mutex and condition variables belonging to thread pool    
    pthread_mutex_destroy(&pool->poolMutex);
    pthread_cond_destroy(&pool->newTaskAvailable);
    //Free the thread pool and return NULL
    free(pool);
}

/*
* Helper function that determines whether submit is called on an internal or external task
* if it is an external task it adds to the queue of that worker
*/
static worker* checkForWorker(thread_pool *pool, future *futureVar) 
{
    struct list_elem *currThread;
    worker* result = NULL;
    pthread_t myThreadID = pthread_self();
    for(currThread = list_begin(&pool->workerThreads); currThread != list_end(&pool->workerThreads); currThread =list_next(currThread)) 
    {
        worker *checkWorker = list_entry(currThread,struct worker, elem);
        pthread_mutex_lock(&checkWorker->tMutex);
        if (checkWorker->threadID == myThreadID) {
            //If this is an internal task add to the queue of the thread that belongs to it
            futureVar->type = INTERNAL;
            futureVar->result = futureVar->task(pool, futureVar->args);
            sem_post(&futureVar->futureSem);
            futureVar->status=COMPLETE;
            result = checkWorker;            
        }
        pthread_mutex_unlock(&checkWorker->tMutex);
    }
    return result;    
}

/* 
 * Submit a fork join task to the thread pool and return a
 * future.  The returned future can be used in future_get()
 * to obtain the result.
 * 'pool' - the pool to which to submit
 * 'task' - the task to be submitted.
 * 'data' - data to be passed to the task's function
 *
 * Returns a future representing this computation.
 */
struct future * thread_pool_submit(struct thread_pool *pool, fork_join_task_t task, void * data)
{
    pthread_mutex_lock(&pool->poolMutex);
    if (pool == NULL) 
    {
        printf("Cannot pass NULL arguement for pool\n");
        return NULL;
    }
    if (task == NULL) 
    {
        printf("Cannot pass NULL arguement for task\n");
        return NULL;
    }    
    //Create the future variable to return
    future *futureVar = malloc(sizeof(future));
    futureVar->task = task;
    futureVar->args = data;
    futureVar->result = NULL;
    futureVar->status = BLOCKED;
    pthread_mutex_init(&futureVar->futureMutex, NULL);
    sem_init(&futureVar->futureSem, 0, 0);  
    //Check if there are any idle worker threads and add to the workers queue
    worker* idleWorker = checkForWorker(pool, futureVar);
    //Place in global queue if it is an external task
    if (idleWorker == NULL) 
    {
        futureVar->type = EXTERNAL;        
        list_push_back(&pool->taskQueue, &futureVar->elem);        
    }
    pool->noNewWork = false;
    pthread_cond_signal(&pool->newTaskAvailable);
    pthread_mutex_unlock(&pool->poolMutex);    
    return futureVar;
}


/* Make sure that the thread pool has completed the execution
 * of the fork join task this future represents.
 *
 * Returns the value returned by this task.
 */
void * future_get(struct future *task)
{
    //Ensure that the task is complete before returning a value
    pthread_mutex_lock(&task->futureMutex);
    if (task->status!=COMPLETE)
    {
        pthread_mutex_unlock(&task->futureMutex);       
        sem_wait(&task->futureSem);        
    }
    else {
        pthread_mutex_unlock(&task->futureMutex);
    }
    return task->result;
}

/* Deallocate this future.  Must be called after future_get() */
void future_free(struct future *task)
{
    pthread_mutex_destroy(&task->futureMutex);
    sem_destroy(&task->futureSem);    
    free(task);
}
