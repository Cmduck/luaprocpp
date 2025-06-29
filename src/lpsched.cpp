/*
** scheduler module for executing lua processes
** See Copyright Notice in luaproc.h
*/

#include <thread>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

#ifdef __cplusplus
extern "C" {
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}
#endif // __cplusplus

#include "lpsched.h"
#include "luaproc.h"

#define FALSE 0
#define TRUE !FALSE
#define LUAPROC_SCHED_WORKERS_TABLE "workertb"

#if (LUA_VERSION_NUM >= 502)
#define luaproc_resume(L, from, nargs) lua_resume(L, from, nargs)
#else
#define luaproc_resume(L, from, nargs) lua_resume(L, nargs)
#endif

/********************
 * global variables *
 *******************/

/* ready process list */
static list ready_lp_list;

/* ready process queue access mutex */
static std::mutex mutex_sched;

/* active luaproc count access mutex */
static std::mutex mutex_lp_count;

/* wake worker up conditional variable */
static std::condition_variable cond_wakeup_worker;

/* no active luaproc conditional variable */
static std::condition_variable cond_no_active_lp;

static std::vector<std::thread> worker_threads(8); /* vector to hold worker threads */

static std::unordered_map<std::thread::id, std::thread*> thread_map;

/* lua_State used to store workers hash table */
static lua_State *workerls = NULL;

static int lpcount = 0;        /* number of active luaprocs */
static int workerscount = 0;   /* number of active workers */
static int destroyworkers = 0; /* number of workers to destroy */

/***********************
 * register prototypes *
 ***********************/

static void sched_dec_lpcount(void);

/*******************************
 * worker thread main function *
 *******************************/

/* worker thread main function */
void *workermain(void *args)
{

  luaproc *lp;
  int procstat;

  /* main worker loop */
  while (TRUE)
  {
    /*
      wait until instructed to wake up (because there's work to do
      or because workers must be destroyed)
    */
      std::unique_lock<std::mutex> lock(mutex_sched);
      // Wait until there's work or we need to destroy workers
      cond_wakeup_worker.wait(lock, []() { 
            return list_count(&ready_lp_list) > 0 || destroyworkers > 0;
          });


    if (destroyworkers > 0)
    { /* check whether workers should be destroyed */

      destroyworkers--; /* decrease workers to be destroyed count */
      workerscount--;   /* decrease active workers count */

      /* remove worker from workers table */
	  auto thread_self = thread_map[std::this_thread::get_id()];
	  std::cout << "workermain destroying worker thread: " << std::this_thread::get_id() << std::endl;
      lua_getglobal(workerls, LUAPROC_SCHED_WORKERS_TABLE);
      lua_pushlightuserdata(workerls, (void*)thread_self);
      lua_pushnil(workerls);
      lua_rawset(workerls, -3);
      lua_pop(workerls, 1);

      cond_wakeup_worker.notify_one(); /* wake other workers up */
      lock.unlock();
      return nullptr;
    }

    /* remove lua process from the ready queue */
    lp = list_remove(&ready_lp_list);
    lock.unlock();

    /* execute the lua code specified in the lua process struct */
    procstat = luaproc_resume(luaproc_get_state(lp), NULL,
                              luaproc_get_numargs(lp));
    /* reset the process argument count */
    luaproc_set_numargs(lp, 0);

    /* has the lua process sucessfully finished its execution? */
    if (procstat == 0)
    {
      luaproc_set_status(lp, LUAPROC_STATUS_FINISHED);
      luaproc_recycle_insert(lp); /* try to recycle finished lua process */
      sched_dec_lpcount();        /* decrease active lua process count */
    }

    /* has the lua process yielded? */
    else if (procstat == LUA_YIELD)
    {

      /* yield attempting to send a message */
      if (luaproc_get_status(lp) == LUAPROC_STATUS_BLOCKED_SEND)
      {
        luaproc_queue_sender(lp); /* queue lua process on channel */
        /* unlock channel */
        luaproc_unlock_channel(luaproc_get_channel(lp));
      }

      /* yield attempting to receive a message */
      else if (luaproc_get_status(lp) == LUAPROC_STATUS_BLOCKED_RECV)
      {
        luaproc_queue_receiver(lp); /* queue lua process on channel */
        /* unlock channel */
        luaproc_unlock_channel(luaproc_get_channel(lp));
      }

      /* yield on explicit coroutine.yield call */
      else
      {
        /* re-insert the job at the end of the ready process queue */
        mutex_sched.lock();
        list_insert(&ready_lp_list, lp);
        mutex_sched.unlock();
      }
    }

    /* or was there an error executing the lua process? */
    else
    {
      /* print error message */
        std::cerr << "close lua_State error:" << luaL_checkstring(luaproc_get_state(lp), -1) << std::endl;
      lua_close(luaproc_get_state(lp)); /* close lua state */
      sched_dec_lpcount();              /* decrease active lua process count */
    }
  }
}

/***********************
 * auxiliary functions *
 **********************/

/* decrease active lua process count */
static void sched_dec_lpcount(void)
{
  mutex_lp_count.lock();
  lpcount--;
  /* if count reaches zero, signal there are no more active processes */
  if (lpcount == 0)
  {
    cond_no_active_lp.notify_one();
  }
  mutex_lp_count.unlock();
}

/**********************
 * exported functions *
 **********************/

/* increase active lua process count */
void sched_inc_lpcount(void)
{
  mutex_lp_count.lock();
  lpcount++;
  mutex_lp_count.unlock();
}

/* local scheduler initialization */
int sched_init(void)
{

  int i;

  /* initialize ready process list */
  list_init(&ready_lp_list);

  /* initialize workers table and lua_State used to store it */
  workerls = luaL_newstate();
  lua_newtable(workerls);
  lua_setglobal(workerls, LUAPROC_SCHED_WORKERS_TABLE);

  /* get ready to access worker threads table */
  lua_getglobal(workerls, LUAPROC_SCHED_WORKERS_TABLE);

  /* create default number of initial worker threads */
  for (i = 0; i < LUAPROC_SCHED_DEFAULT_WORKER_THREADS; i++)
  {
      std::thread* worker = nullptr;
      try
      {
		  worker = new std::thread(workermain, nullptr);
		  thread_map[worker->get_id()] = worker; // Store the thread in the map
          /* store worker thread id in a table */
          lua_pushlightuserdata(workerls, (void*)worker);
          lua_pushboolean(workerls, TRUE);
          lua_rawset(workerls, -3);

          workerscount++; /* increase active workers count */
      }
      catch (const std::exception&)
      {
		  if (worker) delete worker; // Clean up if creation failed
          lua_pop(workerls, 1); /* pop workers table from stack */
          return LUAPROC_SCHED_PTHREAD_ERROR;
      }
  }

  lua_pop(workerls, 1); /* pop workers table from stack */

  return LUAPROC_SCHED_OK;
}

/* set number of active workers */
int sched_set_numworkers(int numworkers)
{

  int i, delta;
  mutex_sched.lock();

  /* calculate delta between existing workers and set number of workers */
  delta = numworkers - workerscount;

  /* create additional workers */
  if (numworkers > workerscount)
  {

    /* get ready to access worker threads table */
    lua_getglobal(workerls, LUAPROC_SCHED_WORKERS_TABLE);

    /* create additional workers */
    for (i = 0; i < delta; i++)
    {
        try
        {
            std::thread* worker = new std::thread(workermain, nullptr);
            thread_map[worker->get_id()] = worker; // Store the thread in the map
            /* store worker thread id in a table */
            lua_pushlightuserdata(workerls, (void*)worker);
            lua_pushboolean(workerls, TRUE);
            lua_rawset(workerls, -3);

            workerscount++; /* increase active workers count */
        }
        catch (const std::exception& e)
        {
            mutex_sched.unlock();
            lua_pop(workerls, 1); /* pop workers table from stack */
            return LUAPROC_SCHED_PTHREAD_ERROR;
        }
    }

    lua_pop(workerls, 1); /* pop workers table from stack */
  }
  /* destroy existing workers */
  else if (numworkers < workerscount)
  {
    destroyworkers = destroyworkers + numworkers;
  }

  mutex_sched.unlock();

  return LUAPROC_SCHED_OK;
}

/* return the number of active workers */
int sched_get_numworkers(void)
{

  int numworkers;

  mutex_sched.lock();
  numworkers = workerscount;
  mutex_sched.unlock();

  return numworkers;
}

/* insert lua process in ready queue */
void sched_queue_proc(luaproc *lp)
{
	mutex_sched.lock();
  list_insert(&ready_lp_list, lp); /* add process to ready queue */
  /* set process status ready */
  luaproc_set_status(lp, LUAPROC_STATUS_READY);
  cond_wakeup_worker.notify_one(); /* wake worker up */
  mutex_sched.unlock();
}

/* join worker threads (called when Lua exits). not joining workers causes a
   race condition since lua_close unregisters dynamic libs with dlclose and
   thus libpthreads can be unloaded while there are workers that are still
   alive. */
void sched_join_workers(void)
{

  lua_State *L = luaL_newstate();
  const char *wtb = "workerstbcopy";

  /* wait for all running lua processes to finish */
  sched_wait();

  /* initialize new state and create table to copy worker ids */
  lua_newtable(L);
  lua_setglobal(L, wtb);
  lua_getglobal(L, wtb);

  mutex_sched.lock();

  /* determine remaining active worker threads and copy their ids */
  lua_getglobal(workerls, LUAPROC_SCHED_WORKERS_TABLE);
  lua_pushnil(workerls);
  while (lua_next(workerls, -2) != 0)
  {
    lua_pushlightuserdata(L, lua_touserdata(workerls, -2));
    lua_pushboolean(L, TRUE);
    lua_rawset(L, -3);
    /* pop value, leave key for next iteration */
    lua_pop(workerls, 1);
  }

  /* pop workers copy table name from stack */
  lua_pop(L, 1);

  /* set all workers to be destroyed */
  destroyworkers = workerscount;

  /* wake workers up */
  cond_wakeup_worker.notify_one();
  mutex_sched.unlock();

  /* join with worker threads (read ids from local table copy ) */
  lua_getglobal(L, wtb);
  lua_pushnil(L);
  while (lua_next(L, -2) != 0)
  {
    auto pthread = static_cast<std::thread*>(lua_touserdata(L, -2));
	thread_map.erase(pthread->get_id()); // Remove from thread map
	std::cout << "Joining thread: " << pthread->get_id() << std::endl;
    pthread->join(); // Join the thread
	delete pthread; // Clean up the thread object

    /* pop value, leave key for next iteration */
    lua_pop(L, 1);
  }
  lua_pop(L, 1);

  lua_close(workerls);
  lua_close(L);
}

/* wait until there are no more active lua processes and active workers. */
void sched_wait(void)
{

  /* wait until there are not more active lua processes */
    std::unique_lock<std::mutex> lock(mutex_lp_count);
  if (lpcount != 0)
  {
      cond_no_active_lp.wait(lock, [] { return lpcount == 0; });
  }
  lock.unlock();
}
