#include <mutex>
#include <condition_variable>
#include <queue>
#include <thread>
#include <memory>
#include "hnsw.h"
const int KNN_QUERY_MESSAGE = 1;
const int RANGE_QUERY_MESSAGE = 2;
const int KNN_QUERY_HNSW_INIT_MESSAGE = 3;

class Message{
    public:
        Message(int query_type_,int a3v_id_,std::string &path_key_,std::shared_ptr<std::vector<float>> query_point_,int k_,float radius_,std::shared_ptr<std::vector<int>> dimensions_,std::shared_ptr<std::vector<float>> weights_):
        query_type(query_type_),a3v_id(a3v_id_),path_key(path_key_),query_point(query_point_),k(k_),radius(radius_),dimensions(dimensions_),weights(weights_){
        }
    public:
        // range query or knn query.
        int query_type;
        // A3V Index id
        int a3v_id;
        // a3v index key
        std::string path_key;
        // query vector
        std::shared_ptr<std::vector<float>> query_point;
        // weights
        std::shared_ptr<std::vector<float>> weights;
        // top-k
        int k;
        // radius
        float radius;
        // dimensions
        std::shared_ptr<std::vector<int>>  dimensions;
        MultiColumnHnsw* send_hard_hnsws;
};

// the message is the a3v index id and query point,
extern std::mutex mtx;
extern std::condition_variable cv;
extern std::queue<std::shared_ptr<Message>> channel;

void A3vAsyncRecieveServer();
void A3vAsyncSendServer(std::shared_ptr<Message> message);
