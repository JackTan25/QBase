#include "a3v_async_server.h"
#include "init.h"
#include <iostream>
std::mutex mtx;
std::condition_variable cv;
std::queue<std::shared_ptr<Message>> channel;

// async a3v recieve server
void A3vAsyncRecieveServer() {
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, []{ return !channel.empty(); }); 
        std::shared_ptr<Message> data = channel.front();
        channel.pop();
        lock.unlock(); 
        if(!memory_init.memory_indexes.count(data->path_key)){
            elog(INFO,"can't find path_key %s in A3vAsyncRecieveServer",data->path_key.c_str());
        }
        if(memory_init.memory_indexes[data->path_key].size() - 1 < data->a3v_id){
            elog(ERROR,"can't find a3v_id(overflow) %d in A3vAsyncRecieveServer",data->a3v_id);
        }
        // carefully open elog for async_server, it will affect the postgres clien results.
        // elog(INFO,"get a a3v message");
        auto a3v_index = memory_init.memory_indexes[data->path_key][data->a3v_id];
        if(a3v_index->query_records.load() < A3V_HINT_QUERY_RECORDS){
            if(data->query_type == KNN_QUERY_MESSAGE){
                std::priority_queue<PQNode> result_pq;
                a3v_index->KnnCrackSearch(*data->weights,data->query_point->data(),data->k,result_pq,*data->dimensions,a3v_index->last_top_k_mean);
            }else{
                std::vector<PQNode> result_ids;
                a3v_index->RangeCrackSearch(*data->weights,data->query_point->data(),data->radius,result_ids,*data->dimensions);
            }
            a3v_index->query_records.fetch_add(1);
        }
        // elog(LOG,"finish a3v messgae processing");
    }
}

// async a3v send server
void A3vAsyncSendServer(std::shared_ptr<Message> message) {
    elog(INFO,"Send a message");
    // auto begin_query = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(mtx);
        auto weights = message->weights;
        channel.push(message); 
    }
    cv.notify_one();
    // auto end_query = std::chrono::steady_clock::now();
    // std::cout<<"time cost "<<std::chrono::duration<double, std::milli>(end_query - begin_query).count()<<"millseconds"<<std::endl;
}
