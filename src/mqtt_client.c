/***********************************************************
*  File: mqtt_client.c
*  Author: nzy
*  Date: 20150526
***********************************************************/
#define __MQTT_CLIENT_GLOBALS
#include "mqtt_client.h"
#include <string.h>
#include "libemqtt.h"
#include "com_mmod.h"
#include "uni_module.h"
#include "tuya_tls.h"
#include "uni_log.h"
#include "tuya_os_adapter.h"
#include "tuya_base_utilities.h"
#include "mem_pool.h"
#include "log_seq_mqtt.h"

/***********************************************************
*************************micro define***********************
***********************************************************/
#define RESP_TIMEOUT 42 // sec (DEF_MQTT_ALIVE_TIME_S * 0.7)
#define DEF_MQTT_ALIVE_TIME_S 60
#define DEF_MQ_RECV_BUF_LEN 512
#define MS_PACT_INTR 500 // mqtt publish async check time interval
#define PUB_RESP_TO_LMT 3 // publish respond timeout limit

//#define PRE_TOPIC "smart/device/in/"
//#define PRE_TOPIC_LEN sizeof(PRE_TOPIC)
//#define MQ_SERV_TOPIC "smart/device/out/"
//#define PRE_CLOUD_TOPIC_LEN sizeof(MQ_SERV_TOPIC)

// mqtt 状态机执行
typedef enum {
    MQTT_GET_SERVER_IP = 0,
    MQTT_SOCK_CONN,
    MQTT_CONNECT,
    MQ_CONN_RESP,
    MQTT_SUBSCRIBE,
    MQ_SUB_RESP,
    MQTT_SUB_FINISH,
    MQTT_REC_MSG,
    MQTT_QUIT_BEGIN,
    MQTT_QUIT_FINISH,
}MQTT_STATUS_MACHINE;

typedef enum {
   MQTT_PARSE_CONNECT = 0,
   MQTT_PARSE_SUBSCRIBE,
   //MQTT_PARSE_MSG_PUBLISH,
}MQTT_PARSE_TYPE;

#define MQTT_HEAD_SIZE 4

typedef struct {
    // USHORT_T index;
    USHORT_T msg_id;
    MQ_PUB_ASYNC_IFM_CB cb;
    VOID *prv_data;
    TIME_S time;
    TIME_S to_lmt;
}MQ_MSG_DAT_ELE_S;

#define CONC_MQM_NUM 8
typedef struct {
    BYTE_T b_use[( CONC_MQM_NUM/8 + ((CONC_MQM_NUM%8)?1:0) )];
    MQ_MSG_DAT_ELE_S mqmde_tbl[CONC_MQM_NUM];
}MQ_PUB_ASYNC_S;

#define SND_TO_CMT_LMT 3
#define MAX_MQTT_TOPIC_NUM  2
typedef struct {
    mqtt_broker_handle_t mq;
    tuya_tls_hander tls_hander;
    BOOL_T enable_tls;
    INT_T fd;

    UNW_IP_ADDR_T serv_ip;
    USHORT_T serv_port;
#if defined(GW_SUPPORT_WIRED_WIFI) && (GW_SUPPORT_WIRED_WIFI==1)
    UNW_IP_ADDR_T client_ip;
#endif

    MQ_DATA_RECV_CB recv_cb;
    MQ_CONNED_CB conn_cb;
    MQ_DISCONN_CB dis_conn_cb;
    MQ_CONN_DENY_CB conn_deny_cb;
    MQ_PERMIT_CONN_CB permit_comm_cb;
    MQ_UPDATE_AUTH_CB update_auth_cb;

    THRD_HANDLE thread;
    MUTEX_HANDLE mutex;
    MUTEX_HANDLE data_mutex;
    TM_MSG_S *alive_tmm;
    TIMER_ID resp_tm;

    MQTT_STATUS_MACHINE status;
    BOOL_T mq_connect;

    CHAR_T **domain_tbl;
    BYTE_T domain_num;

    BYTE_T domain_index;
    BYTE_T conn_retry_cnt;
    BYTE_T lcr_cnt; // logic connect retry count
    CHAR_T *subcribe_topic[MAX_MQTT_TOPIC_NUM];
    BYTE_T recv_buf[DEF_MQ_RECV_BUF_LEN];
    USHORT_T recv_in;
    USHORT_T recv_out;

    UINT_T send_timeout_cnt;

    TIMER_ID pact_tm;
    MQ_PUB_ASYNC_S mpa;
    UINT_T fail_cnt;
    UINT_T success_cnt;


    MUTEX_HANDLE can_send_mutex;
    BOOL_T can_send; //TRUE:可以发送，FALSE:不可以发送

    BOOL_T enable_wakeup;
    CHAR_T **wakeup_domain_tbl;
    BYTE_T wakeup_domain_num;
    USHORT_T wakeup_port;

    CHAR_T **working_domain_tbl;
    BYTE_T working_domain_num;
    USHORT_T working_serv_port;
    BYTE_T heart_beat_lost;
}MQ_CNTL_S;

/***********************************************************
*************************variable define********************
***********************************************************/
STATIC INT_T s_permit_retry_interval = 1000;

/***********************************************************
*************************function define********************
***********************************************************/
STATIC INT_T __mq_send(void* socket_info, const void* buf,unsigned int count);
STATIC OPERATE_RET __mq_recv(MQ_CNTL_S *mq, OUT BYTE_T *buf,IN CONST UINT_T count,OUT UINT_T *recv_len);
STATIC VOID __mq_close(IN CONST MQ_HANDLE hand);
STATIC OPERATE_RET __mq_client_sock_conn(IN CONST MQ_HANDLE hand);
STATIC VOID __mq_ctrl_task(PVOID_T pArg);
#if defined(ENABLE_IPC) && (LOW_POWER_ENABLE==1)
STATIC VOID __mq_ctrl_task_lp(PVOID_T pArg);
#endif
STATIC VOID __set_mqtt_conn_stat(INOUT MQ_CNTL_S *mq,IN CONST BOOL_T connect);
#if 0
STATIC INT_T __find_free_mqmde_tbl_and_set(INOUT MQ_CNTL_S *mq);
STATIC VOID __set_mqmde_tbl_free(INOUT MQ_CNTL_S *mq,IN CONST INT_T index);
#endif
STATIC VOID __pub_ack_proc(INOUT MQ_CNTL_S *mq,IN CONST USHORT_T msg_id);
STATIC VOID __pub_ack_timeout_proc(INOUT MQ_CNTL_S *mq);

STATIC VOID __mqtt_log_seq_err(UINT_T id, void* val, LS_DATA_TP_T type)
{
#if defined(WIFI_GW) && (WIFI_GW==1)
    SCHAR_T rssi = 0;
    OPERATE_RET ret = wf_station_get_conn_ap_rssi(&rssi);
    if(ret == OPRT_OK) {
        INSERT_LOG_SEQ_DEC(LOGSEQ_MQTT_RSSI, rssi);
    }
#endif

    if(type == LDT_NULL) {
        INSERT_ERROR_LOG_SEQ_NULL(id);
    } else if(type == LDT_HEX) {
        INSERT_ERROR_LOG_SEQ_HEX(id, (UINT_T)val);
    } else if(type == LDT_DEC) {
        INSERT_ERROR_LOG_SEQ_DEC(id, (INT_T)val);
    } else if(type == LDT_TIMESTAMP) {
        INSERT_ERROR_LOG_SEQ_TM(id, (TIME_T)val);
    } else if(type == LDT_STRING) {
        INSERT_ERROR_LOG_SEQ_STR(id, ((CHAR_T *)val));
    }
    
}

OPERATE_RET mqtt_keep_alive(IN CONST MQ_HANDLE hand)
{
    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    return mqtt_ping(&mq->mq);
}

/* 定时保活 消息回调函数 */
STATIC VOID __alive_tm_msg_cb(struct s_tm_msg *tm_msg)
{
    MQ_CNTL_S *mq = (MQ_CNTL_S *)(tm_msg->data);

    PR_DEBUG("mqtt_ping -->>");

    sys_start_timer(mq->resp_tm,RESP_TIMEOUT*1000,TIMER_ONCE);
    INT_T ret = mqtt_ping(&mq->mq);
    if(ret != 1)  {
        PR_ERR("mqtt_ping err:%d",tuya_hal_net_get_errno());
        __mqtt_log_seq_err(LOGSEQ_MQTT_PING, tuya_hal_net_get_errno(), LDT_DEC);
        if(UNW_ENOMEM != tuya_hal_net_get_errno()) {
            sys_stop_timer(mq->resp_tm);
            __mq_close(mq);
            PR_DEBUG("mqtt_ping <<--");
            return;
        }
    }

    // ping成功，记录结果与时间
    //INSERT_LOG_SEQ_TM(LOGSEQ_MQTT_TIME, uni_time_get_posix());
    //INSERT_LOG_SEQ_TM(LOGSEQ_MQTT_PING, ret);
    PR_DEBUG("mqtt_ping <<--");
}

/* 掉线后超时回调，ping不通的情况下 */
STATIC VOID __resp_tm_cb(UINT_T timerID,PVOID_T pTimerArg)
{
    MQ_CNTL_S *mq = (MQ_CNTL_S *)(pTimerArg);

    __mqtt_log_seq_err(LOGSEQ_MQTT_PING_TIMEOUT, uni_time_get_posix(), LDT_TIMESTAMP);

    PR_NOTICE("respond timeout -->>");

    tuya_hal_mutex_lock(mq->mutex);
    mq->heart_beat_lost++;
    tuya_hal_mutex_unlock(mq->mutex);

    __mq_close(mq);

    PR_NOTICE("respond timeout <<--");

}

/* qos = 1的包的超时相应 */
STATIC VOID __pact_tm_cb(UINT_T timerID,PVOID_T pTimerArg)
{
    MQ_CNTL_S *mq = (MQ_CNTL_S *)(pTimerArg);

    __pub_ack_timeout_proc(mq);
}

/***********************************************************
*  Function: create_mqtt_hand
*  Input: domain_tbl
*         domain_num
*         serv_port
*         mqc_if->mqtt client info
*         s_alive->if(0 == s_alive) then set DEF_MQTT_ALIVE_TIME_S
*  Output: hand
*  Return: OPERATE_RET
***********************************************************/
OPERATE_RET create_mqtt_hand(IN CONST CHAR_T **domain_tbl,\
                             IN CONST BYTE_T domain_num,\
                             IN CONST USHORT_T serv_port,\
                             IN CONST BOOL_T  enable_tls,\
                             IN CONST MQ_CLIENT_IF_S *mqc_if,\
                             IN CONST USHORT_T s_alive,\
                             IN CONST MQ_DATA_RECV_CB recv_cb,\
                             IN CONST CHAR_T **wakeup_domain_tbl,\
                             IN CONST BYTE_T wakeup_domain_num,\
                             IN CONST USHORT_T wakeup_serv_port,\
                             OUT MQ_HANDLE *hand)
{
    if(NULL == recv_cb || NULL == hand || NULL == mqc_if || \
       NULL == mqc_if->subcribe_topic || NULL == mqc_if->client_id || \
       NULL == mqc_if->user_name || NULL == domain_tbl || 0 == domain_num) {
        PR_ERR("para null");
        return OPRT_INVALID_PARM;
    }

    MQ_CNTL_S *mq = Malloc(SIZEOF(MQ_CNTL_S));
    if(NULL == mq) {
        PR_ERR("malloc fail");
        return OPRT_MALLOC_FAILED;
    }
    memset(mq,0,SIZEOF(MQ_CNTL_S));

    PR_DEBUG("subcribe_topic:%s, client_id:%s, user_name:%s, passwd:%s", mqc_if->subcribe_topic, 
        mqc_if->client_id, mqc_if->user_name, mqc_if->passwd);

    OPERATE_RET op_ret = OPRT_OK;
    mq->subcribe_topic[0] = Malloc( strlen(mqc_if->subcribe_topic)+1 );
    if(NULL == mq->subcribe_topic[0]) {
        op_ret = OPRT_MALLOC_FAILED;
        goto ERR_EXIT;
    }
    strncpy(mq->subcribe_topic[0], mqc_if->subcribe_topic, strlen(mqc_if->subcribe_topic)+1 );

    mq->domain_tbl = Malloc(SIZEOF(CHAR_T *)*domain_num);
    if(NULL == mq->domain_tbl) {
        op_ret = OPRT_MALLOC_FAILED;
        goto ERR_EXIT;
    }
    memset(mq->domain_tbl,0,(SIZEOF(CHAR_T *)*domain_num));
    mq->domain_num = domain_num;
    mq->serv_port = serv_port;
    mq->enable_tls = enable_tls;

    INT_T i = 0;
    for(i = 0;i < mq->domain_num;i++) {
        mq->domain_tbl[i] = Malloc( strlen(domain_tbl[i])+1 );
        if(NULL == mq->domain_tbl[i]) {
            op_ret = OPRT_MALLOC_FAILED;
            goto ERR_EXIT;
        }
        strncpy(mq->domain_tbl[i], domain_tbl[i], strlen(domain_tbl[i])+1 );

        CHAR_T *p_colon = strstr(mq->domain_tbl[i], ":");
        if(p_colon != NULL) {
            PR_DEBUG("domain<%d> %s HAVE PORT", i, mq->domain_tbl[i] );
            *p_colon = 0;
            mq->serv_port = atoi(p_colon + 1);
            PR_DEBUG("PARSE %s:%d", mq->domain_tbl[i], mq->serv_port);
        }
    }

    mq->working_domain_tbl = mq->domain_tbl;
    mq->working_domain_num = mq->domain_num;
    mq->working_serv_port = mq->serv_port;

    if(wakeup_domain_num != 0) {
        mq->wakeup_domain_tbl = Malloc(SIZEOF(CHAR_T *)*wakeup_domain_num);
        if(NULL == mq->wakeup_domain_tbl) {
            op_ret = OPRT_MALLOC_FAILED;
            goto ERR_EXIT;
        }
        memset(mq->wakeup_domain_tbl,0,(SIZEOF(CHAR_T *)*wakeup_domain_num));
        mq->wakeup_domain_num = wakeup_domain_num;
        mq->wakeup_port = wakeup_serv_port;

        for(i = 0;i < mq->wakeup_domain_num;i++) {
            mq->wakeup_domain_tbl[i] = Malloc(strlen(wakeup_domain_tbl[i])+1);
            if(NULL == mq->wakeup_domain_tbl[i]) {
                op_ret = OPRT_MALLOC_FAILED;
                goto ERR_EXIT;
            }
            strncpy(mq->wakeup_domain_tbl[i],wakeup_domain_tbl[i], strlen(wakeup_domain_tbl[i])+1);

            CHAR_T *p_colon = strstr(mq->wakeup_domain_tbl[i], ":");
            if(p_colon != NULL) {
                PR_DEBUG("wakeup_domain_tbl<%d> %s HAVE PORT", i, mq->wakeup_domain_tbl[i] );
                *p_colon = 0;
                mq->wakeup_port = atoi(p_colon + 1);
                PR_DEBUG("PARSE %s:%d", mq->wakeup_domain_tbl[i], mq->wakeup_port);
            }
        }
    }


    op_ret = tuya_hal_mutex_create_init(&mq->mutex);
    if(OPRT_OK != op_ret) {
        goto ERR_EXIT;
    }

    op_ret = tuya_hal_mutex_create_init(&mq->data_mutex);
    if(OPRT_OK != op_ret) {
        goto ERR_EXIT;
    }

    op_ret = tuya_hal_mutex_create_init(&mq->can_send_mutex);
    if(OPRT_OK != op_ret) {
        goto ERR_EXIT;
    }

    op_ret = cmmod_cr_tm_msg_hand(__alive_tm_msg_cb,mq,&(mq->alive_tmm));
    if(OPRT_OK != op_ret) {
        goto ERR_EXIT;
    }

    op_ret = sys_add_timer(__resp_tm_cb,mq,&(mq->resp_tm));
    if(OPRT_OK != op_ret) {
        goto ERR_EXIT;
    }

    op_ret = sys_add_timer(__pact_tm_cb,mq,&(mq->pact_tm));
    if(OPRT_OK != op_ret) {
        goto ERR_EXIT;
    }

    mqtt_init(&mq->mq,mqc_if->client_id);
    mqtt_init_auth(&mq->mq,mqc_if->user_name,mqc_if->passwd);
    if(0 == s_alive) {
        mqtt_set_alive(&mq->mq,DEF_MQTT_ALIVE_TIME_S);
    }else {
        mqtt_set_alive(&mq->mq,s_alive);
    }

    mq->mq.sendBuf = __mq_send;
    mq->recv_cb = recv_cb;
    *hand = (MQ_HANDLE *)mq;

    return OPRT_OK;

ERR_EXIT:
    release_mqtt_hand((MQ_HANDLE *)mq);
    return op_ret;
}

OPERATE_RET mqtt_update_auth(IN CONST MQ_HANDLE hand, IN CONST CHAR_T *p_username, IN CONST CHAR_T *p_passwd)
{
    if(NULL == hand) {
        return OPRT_INVALID_PARM;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    mqtt_init_auth(&mq->mq, p_username, p_passwd);

    return OPRT_OK;
}


/***********************************************************
*  Function: mqtt_register_cb
*  Input: hand
*         conn_cb
*         dis_conn_cb
*         conn_deny_cb
*         permit_conn_cb
*  Output: none
*  Return: OPERATE_RET
***********************************************************/
OPERATE_RET mqtt_register_cb(IN CONST MQ_HANDLE hand,\
                             IN CONST MQ_CONNED_CB conn_cb,\
                             IN CONST MQ_DISCONN_CB dis_conn_cb,\
                             IN CONST MQ_CONN_DENY_CB conn_deny_cb,\
                             IN CONST MQ_PERMIT_CONN_CB permit_conn_cb, \
                             IN CONST MQ_UPDATE_AUTH_CB update_auth_cb)
{
    if(NULL == hand) {
        return OPRT_INVALID_PARM;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    mq->conn_cb = conn_cb;
    mq->dis_conn_cb = dis_conn_cb;
    mq->conn_deny_cb = conn_deny_cb;
    mq->permit_comm_cb = permit_conn_cb;
    mq->update_auth_cb = update_auth_cb;

    return OPRT_OK;
}

/***********************************************************
*  Function: mqtt_client_start
*  Input: hand
*  Output: none
*  Return: OPERATE_RET
***********************************************************/
OPERATE_RET mqtt_client_start(IN CONST MQ_HANDLE hand,IN CONST CHAR_T *name)
{
    if(NULL == hand) {
        return OPRT_INVALID_PARM;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    OPERATE_RET op_ret = OPRT_OK;

    THRD_PARAM_S thrd_param;
    thrd_param.thrdname = (CHAR_T *)name; //"mq_cntl_task"
    thrd_param.stackDepth = 2048+512+512; // change by maht: increase 512 to fix overflow issue
#if defined(ENABLE_NXP_SE050) && (ENABLE_NXP_SE050==1)
    thrd_param.stackDepth += 8192;
#endif
    if(mq->enable_tls) {
        thrd_param.stackDepth += 1024;
    }

    thrd_param.priority = TRD_PRIO_1;
    op_ret = CreateAndStart(&(mq->thread),NULL,NULL,\
                            __mq_ctrl_task,mq,&thrd_param);
    if(OPRT_OK != op_ret) {
        return op_ret;
    }

    return op_ret;
}

/***********************************************************
*  Function: mqtt_client_quit
*  Input: hand
*  Output: none
*  Return: OPERATE_RET
***********************************************************/
__MQTT_CLIENT_EXT \
OPERATE_RET mqtt_client_quit(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand) {
        return OPRT_INVALID_PARM;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    mq->status = MQTT_QUIT_BEGIN;
    PR_NOTICE("mqtt close");
    __mq_close(mq);
    mq->status = MQTT_QUIT_BEGIN;
    tuya_hal_system_sleep(300);

    INT_T index = 0;
    for(index = 0; index < 10; index++) {
        if(mq->status == MQTT_QUIT_FINISH)
            break;

        mq->status = MQTT_QUIT_BEGIN;
        tuya_hal_system_sleep(100);
    }
    PR_DEBUG("mqtt stat:%d", mq->status);
//    release_mqtt_hand(hand);
    return OPRT_OK;
}

/***********************************************************
*  Function: get_mqtt_conn_stat
*  Input: hand
*  Output: none
*  Return: BOOL_T
***********************************************************/
BOOL_T get_mqtt_conn_stat(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand) {
        return FALSE;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    BOOL_T bl = FALSE;

//    tuya_hal_mutex_lock(mq->mutex);
    bl = mq->mq_connect;
//    tuya_hal_mutex_unlock(mq->mutex);

    return bl;
}

STATIC VOID __set_mqtt_conn_stat(INOUT MQ_CNTL_S *mq,IN CONST BOOL_T connect)
{
    tuya_hal_mutex_lock(mq->mutex);
    mq->mq_connect = connect;
    tuya_hal_mutex_unlock(mq->mutex);
}

/***********************************************************
*  Function: release_mqtt_hand
*  Input: hand
*  Output: none
*  Return: none
***********************************************************/
VOID release_mqtt_hand(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand) {
        return;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;

    if(mq->thread) {
        DeleteThrdHandle(mq->thread);
    }

    if(mq->mutex) {
        tuya_hal_mutex_release(mq->mutex);
        mq->mutex = NULL;
    }

    if(mq->data_mutex) {
        tuya_hal_mutex_release(mq->data_mutex);
        mq->data_mutex = NULL;
    }

    if(mq->can_send_mutex) {
        tuya_hal_mutex_release(mq->can_send_mutex);
        mq->data_mutex = NULL;
    }

    UINT_T i;
    for(i=0; i<MAX_MQTT_TOPIC_NUM; i++){
        if(mq->subcribe_topic[i]) {
            Free(mq->subcribe_topic[i]);
        }
    }

    if(mq->alive_tmm) {
        module_release_tm_msg(mq->alive_tmm);
    }

    if(mq->resp_tm) {
        sys_delete_timer(mq->resp_tm);
    }

    if(mq->pact_tm) {
        sys_delete_timer(mq->pact_tm);
    }

    if(mq->domain_tbl) {
        INT_T i = 0;
        for(i = 0;i < mq->domain_num;i++) {
            Free(mq->domain_tbl[i]);
            mq->domain_tbl[i] = NULL;
        }
        Free(mq->domain_tbl);
        mq->domain_tbl = NULL;
    }

    if(mq->wakeup_domain_tbl) {
        INT_T i = 0;
        for(i = 0;i < mq->wakeup_domain_num;i++) {
            Free(mq->wakeup_domain_tbl[i]);
            mq->wakeup_domain_tbl[i] = NULL;
        }
        Free(mq->wakeup_domain_tbl);
        mq->wakeup_domain_tbl = NULL;
    }


    Free(hand);
}

STATIC INT_T __mq_send_raw(MQ_CNTL_S *mq, const void* buf,unsigned int count)
{
    if(NULL == mq->mq.socket_info) {
        PR_DEBUG("err");
        return -1;
    }

    INT_T ret = 0;
    // PR_DEBUG("tuya_hal_net_send start");
    ret = tuya_hal_net_send(mq->fd,buf,count);
    // PR_DEBUG("tuya_hal_net_send end");

    return ret;
}

STATIC OPERATE_RET __mq_recv_raw(MQ_CNTL_S *mq, OUT BYTE_T *buf,\
                                 IN CONST UINT_T count,OUT UINT_T *recv_len)
{
    if(NULL == mq->mq.socket_info) {
        return OPRT_INVALID_PARM;
    }

    INT_T ret = 0;
    INT_T fd = mq->fd;

RETRY:
    ret = tuya_hal_net_recv(fd, buf, count);
    if(ret < 0) {
        UNW_ERRNO_T err = tuya_hal_net_get_errno();
        if(UNW_EWOULDBLOCK == err || \
           UNW_EINTR == err || \
           UNW_EAGAIN == err) {
            PR_ERR("net_recv err:%d errno:%d",err,errno);
            tuya_hal_system_sleep(10);
            goto RETRY;
        }
        PR_ERR("net_recv err:%d errno:%d",err,errno);

        return OPRT_COM_ERROR;
    }
    *recv_len = ret;
    return OPRT_OK;
}

static int __tuya_tls_send_cb( void *ctx, const unsigned char *buf, size_t len )
{
    return __mq_send_raw(ctx, buf, len);
}
static int __tuya_tls_recv_cb( void *ctx, unsigned char *buf, size_t len )
{
    UINT_T recv_len = 0;
    __mq_recv_raw(ctx, buf, len, &recv_len);
    return recv_len;
}

/* todo 要不要用锁包起来*/
STATIC INT_T __mq_send(void* socket_info, const void* buf,unsigned int count)
{
    if(NULL == socket_info)
    {
        PR_DEBUG("err");
        return -1;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)socket_info;

    tuya_hal_mutex_lock(mq->can_send_mutex);
    if(mq->can_send != TRUE) {
        PR_ERR("mq cannot send!");
        tuya_hal_mutex_unlock(mq->can_send_mutex);
        return -1;
    }

    INT_T ret = 0;
    if(mq->enable_tls == TRUE)
    {
        ret = tuya_tls_write(mq->tls_hander, (unsigned char *)buf, count);
    }
    else
    {
        ret = __mq_send_raw(mq, buf, count);
    }

    tuya_hal_mutex_unlock(mq->can_send_mutex);
    return ret;
}

STATIC OPERATE_RET __mq_recv(MQ_CNTL_S *mq, OUT BYTE_T *buf,\
                                  IN CONST UINT_T count,OUT UINT_T *recv_len)
{
    INT_T ret = 0;
    if(mq->enable_tls == TRUE) {
        ret = tuya_tls_read(mq->tls_hander, (unsigned char *)buf, count);
        if(ret < 0) {
            return OPRT_COM_ERROR;
        }
        *recv_len = ret;
        return OPRT_OK;
    }else {
        return __mq_recv_raw(mq,buf,count,recv_len);
    }
}

VOID mqtt_restart(IN CONST MQ_HANDLE hand)
{
    
    //PR_DEBUG("net change will triggle mqtt restart.");
    if(NULL == hand){
        PR_ERR("hand null");
        return;
    }
    
    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    if(TRUE == mq->mq_connect){
        PR_DEBUG("mqtt restart");
        #if 1
        #define ACTIVE_CLOSE_MQ 1
        if(TRUE == IsThisSysTimerRun(mq->resp_tm)){
            sys_stop_timer(mq->resp_tm);
        }
        sys_start_timer(mq->resp_tm, ACTIVE_CLOSE_MQ*1000, TIMER_ONCE);
        #else
        mqtt_client_quit(hand);
        #endif
    }
}

#if defined(GW_SUPPORT_WIRED_WIFI) && (GW_SUPPORT_WIRED_WIFI==1)
UINT_T mqtt_get_socket_ip(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand){
        PR_ERR("hand null.");
        return 0;
    }
    
    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    
    return mq->client_ip;
}
#endif


STATIC VOID __mq_close(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand) {
        return;
    }
    PR_NOTICE("close mqtt -->>");

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;

    tuya_hal_mutex_lock(mq->mutex);
    mq->mq_connect = FALSE;
    mq->recv_in = mq->recv_out = 0;

    if(mq->mq.socket_info)
    {
        INT_T fd = mq->fd;
    #if defined(SHUTDOWN_MODE) && (SHUTDOWN_MODE==1)
        PR_DEBUG("shutdown fd enabled");
        tuya_hal_net_shutdown(fd, UNW_SHUT_RDWR);
    #endif
        // PR_DEBUG("__mq_close start");
        if(fd >= 0){
            tuya_hal_net_close(fd);
            mq->fd = -1;
        }
    #if defined(GW_SUPPORT_WIRED_WIFI) && (GW_SUPPORT_WIRED_WIFI==1)
        mq->client_ip = 0;
    #endif
        mq->mq.socket_info = NULL;

        tuya_hal_mutex_lock(mq->can_send_mutex);
        mq->can_send = FALSE;
        tuya_hal_mutex_unlock(mq->can_send_mutex);

        if(mq->enable_tls == TRUE)
        {
            tuya_tls_disconnect(mq->tls_hander);
            mq->tls_hander = NULL;
        }

        module_stop_tm_msg(mq->alive_tmm);
        if(MQTT_REC_MSG == mq->status && mq->dis_conn_cb) {
            mq->dis_conn_cb();
        }
        if(mq->status != MQTT_QUIT_BEGIN)
            mq->status = MQTT_SOCK_CONN;
    }else {
        if(mq->status != MQTT_QUIT_BEGIN)
            mq->status = MQTT_GET_SERVER_IP;
    }
    tuya_hal_mutex_unlock(mq->mutex);
    PR_NOTICE("close mqtt <<--");

    if(mq->enable_wakeup == TRUE) {
        PR_DEBUG("Disable MQTT Over TLS");
        mq->status = MQTT_GET_SERVER_IP;
        mq->enable_tls = FALSE;

        mq->working_domain_tbl = mq->wakeup_domain_tbl;
        mq->working_domain_num = mq->wakeup_domain_num;
        mq->working_serv_port = mq->wakeup_port;
        mq->domain_index = 0;
    }
}

VOID mq_close(IN CONST MQ_HANDLE hand)
{
    __mq_close(hand);
}

/***********************************************************
*  Function: mq_disconnect
*  Input: hand
*  Output: none
*  Return: none
***********************************************************/
VOID mq_disconnect(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand) {
        return;
    }
    PR_DEBUG("disconnect mqtt");

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    if(mq->mq.socket_info) {
        int ret = mqtt_disconnect(&mq->mq);
        if(ret != 1) {
            PR_ERR("mq disconnect err");
            return;
        }
    }
    __set_mqtt_conn_stat(mq,FALSE);
    tuya_hal_system_sleep(500);
}

STATIC OPERATE_RET __mq_client_sock_conn(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand) {
        return OPRT_INVALID_PARM;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;

    tuya_hal_mutex_lock(mq->mutex);
    mq->mq.socket_info = NULL;

    mq->fd = tuya_hal_net_socket_create(PROTOCOL_TCP);
    if(mq->fd < 0) {
        tuya_hal_mutex_unlock(mq->mutex);
        return OPRT_MID_MQTT_SOCK_CREAT_FAILED;
    }
#if(SYSTEM_RDA5981_2M == OPERATING_SYSTEM)
    USHORT_T mq_lcl_port = 49152 + tuya_hal_get_random_data(65535 - 49152);
    PR_DEBUG("mq local port:%d", mq_lcl_port);
    tuya_hal_net_bind(mq->fd, TY_IPADDR_ANY, mq_lcl_port);
#endif
    // reuse socket port
    if(UNW_SUCCESS != tuya_hal_net_set_reuse(mq->fd)) {
        tuya_hal_net_close(mq->fd);
        mq->fd = -1;
        tuya_hal_mutex_unlock(mq->mutex);
        return OPRT_MID_MQTT_SOCK_SET_FAILED;
    }

    // disable Nagle Algorithm
    if(UNW_SUCCESS != tuya_hal_net_disable_nagle(mq->fd)) {
        tuya_hal_net_close(mq->fd);
        mq->fd = -1;
        tuya_hal_mutex_unlock(mq->mutex);
        return OPRT_MID_MQTT_SOCK_SET_FAILED;
    }

    if(UNW_SUCCESS != tuya_hal_net_set_keepalive(mq->fd,TRUE,(RESP_TIMEOUT+DEF_MQTT_ALIVE_TIME_S),5,1)) {
        tuya_hal_net_close(mq->fd);
        mq->fd = -1;
        tuya_hal_mutex_unlock(mq->mutex);
        return OPRT_MID_MQTT_SOCK_SET_FAILED;
    }
    if(UNW_SUCCESS != tuya_hal_net_set_block(mq->fd,TRUE)) {
        tuya_hal_net_close(mq->fd);
        mq->fd = -1;
		tuya_hal_mutex_unlock(mq->mutex);
        return OPRT_SET_SOCK_ERR;
    }
    PR_DEBUG("serverIP %d port:%d", mq->serv_ip,mq->working_serv_port);
    //Issue: Due to mqtt connect is keeping alive, when client disconnect by AC OFF. The MQTT server still 
    //keep the connect until 3~4 minutes timeout. Before MQTT server connection time out, when MQTT client
    //use same socket port connect to MQTT server, MQTT server will issue a RST which will cause client
    //connect broken and reconnect with a different socket port after local timeout(50 seconds).
    //Solution: Use a different socket port (40000+ random) to avoid to get RST signal to reconnect. 
    if(tuya_hal_net_bind(mq->fd,0,40000+tuya_hal_get_random_data(1000)) < 0) {
        tuya_hal_net_close(mq->fd);
        mq->fd = -1;
        tuya_hal_mutex_unlock(mq->mutex);
        return -3021;//MQTT_TCP_BIND_FAILED;
    }	
    // Connect the socket
    tuya_hal_net_set_timeout(mq->fd,3000,TRANS_SEND);
    if(tuya_hal_net_connect(mq->fd,mq->serv_ip,mq->working_serv_port) < 0) {
        tuya_hal_net_close(mq->fd);
        mq->fd = -1;
        tuya_hal_mutex_unlock(mq->mutex);
        return OPRT_MID_MQTT_TCP_CONNECD_FAILED;
    }

    mq->mq.socket_info = mq;

    if(mq->enable_tls == TRUE)
    {
        PR_DEBUG("mqtt over TLS is enabled. Host:%s Port:%d", mq->working_domain_tbl[mq->domain_index], mq->working_serv_port);
        OPERATE_RET op_ret = OPRT_OK;
        op_ret = tuya_tls_connect(&(mq->tls_hander), mq->working_domain_tbl[mq->domain_index], mq->working_serv_port, 1,
                                  mq, __tuya_tls_send_cb, __tuya_tls_recv_cb,
                                  mq->fd, 10);
        if(OPRT_OK != op_ret) {
            PR_ERR("tuya_tls_connect err:%d",op_ret);

            tuya_tls_disconnect(mq->tls_hander);
            mq->tls_hander = NULL;

			tuya_hal_net_close(mq->fd);
            mq->fd = -1;
            mq->mq.socket_info = NULL;
            tuya_hal_mutex_unlock(mq->mutex);
            return OPRT_MID_MQTT_TCP_TLS_CONNECD_FAILED;
        }
    }
    tuya_hal_mutex_unlock(mq->mutex);
    tuya_hal_mutex_lock(mq->can_send_mutex);
    mq->can_send = TRUE;
#if defined(GW_SUPPORT_WIRED_WIFI) && (GW_SUPPORT_WIRED_WIFI==1)
    tuya_hal_net_get_socket_ip(mq->fd, &mq->client_ip);
#endif
    tuya_hal_mutex_unlock(mq->can_send_mutex);

    return OPRT_OK;
}

STATIC OPERATE_RET __mq_reset_use_table(INOUT MQ_CNTL_S *mq)
{
    tuya_hal_mutex_lock(mq->data_mutex);
    INT_T i;
    for(i = 0;i < CONC_MQM_NUM;i++) {
        if(0 == ((mq->mpa.b_use[i/8] >> i%8) & 0x01)) {
            continue;
        }
        //PR_DEBUG("reset use table:%d", i);
        if(mq->mpa.mqmde_tbl[i].cb) {
            mq->mpa.mqmde_tbl[i].cb(OPRT_MID_MQTT_PUBLISH_TIMEOUT, mq->mpa.mqmde_tbl[i].prv_data);
        }
    }
    memset(mq->mpa.b_use,0, ( CONC_MQM_NUM/8+((CONC_MQM_NUM%8)?1:0) ) );
    tuya_hal_mutex_unlock(mq->data_mutex);
    return OPRT_OK;
}


STATIC OPERATE_RET __mq_logic_conning_proc(INOUT MQ_CNTL_S *mq,IN CONST USHORT_T msg_id)
{
    if(NULL == mq || \
       (((mq->status) != MQ_CONN_RESP && (mq->status) != MQ_SUB_RESP) && (mq->status) != MQTT_SUB_FINISH)) {
        return OPRT_MID_MQTT_INVALID_PARM;
    }

    BYTE_T msg_ack_type = 0;
    if(MQ_CONN_RESP == mq->status) {
        msg_ack_type = MQTT_MSG_CONNACK;
    }else {
        msg_ack_type = MQTT_MSG_SUBACK;
    }

    if(msg_ack_type != MQTTParseMessageType(&mq->recv_buf[0])) {
        PR_ERR("msg_ack_type:%d.",msg_ack_type);
        return OPRT_MID_MQTT_RECV_DATA_FORMAT_WRONG;
    }

    if(MQ_CONN_RESP == mq->status) {        
        // 如果是等待连接应答状态，处理消息，如果数据正常，则进入订阅状态
        if(mq->recv_buf[3] != 0) {
            PR_ERR("mqtt connect resp err:%d",mq->recv_buf[3]);
            if(mq->conn_deny_cb) {
                mq->conn_deny_cb(++mq->lcr_cnt);
            }
            return OPRT_MID_MQTT_RECV_DATA_FORMAT_WRONG;
        }
        mq->lcr_cnt = 0;
        if(mq->status != MQTT_QUIT_BEGIN)
            mq->status = MQTT_SUBSCRIBE;
    }else if(MQTT_SUB_FINISH == mq->status){
        // 如果是订阅完成状态，则进入接受数据状态，并启动keep alive定时器
        mq->status = MQTT_REC_MSG;
        OPERATE_RET op_ret = OPRT_OK;
        op_ret = module_start_tm_msg(mq->alive_tmm,(mq->mq.alive*1000),TIMER_ONCE);
        if(OPRT_OK != op_ret) {
            PR_ERR("start tm err:%d",op_ret);
            return op_ret;
        }

        PR_DEBUG("mqtt subscribe success");

        __set_mqtt_conn_stat(mq, TRUE);
        if(mq->conn_cb) {
            mq->conn_cb();
        }

        if(FALSE == IsThisSysTimerRun(mq->pact_tm)) {
            sys_start_timer(mq->pact_tm,MS_PACT_INTR,TIMER_CYCLE);
        }
    }else {
        // 如果是等待订阅应答状态，更新状态为订阅状态
        if(msg_id != mqtt_parse_msg_id(mq->recv_buf)) {
            PR_ERR("msg_id not match:%d %d",msg_id,mqtt_parse_msg_id(mq->recv_buf));
            return OPRT_MID_MQTT_MSGID_NOT_MATCH;
        }

        if(mq->status != MQTT_QUIT_BEGIN)
            mq->status = MQTT_SUBSCRIBE;
        OPERATE_RET op_ret = OPRT_OK;

        if(mq->enable_wakeup != TRUE) {
            op_ret = module_start_tm_msg(mq->alive_tmm,(mq->mq.alive*1000),TIMER_ONCE);
            if(OPRT_OK != op_ret) {
                PR_ERR("start tm err:%d",op_ret);
                return OPRT_MID_MQTT_START_TM_MSG_ERR;
            }
        }

        PR_NOTICE("mqtt subscribe success");
        mq->success_cnt = 0;

        __set_mqtt_conn_stat(mq, TRUE);

        if(mq->conn_cb) {
            mq->conn_cb();
        }

        if(FALSE == IsThisSysTimerRun(mq->pact_tm)) {
            sys_start_timer(mq->pact_tm,MS_PACT_INTR,TIMER_CYCLE);
        }
    }

    mq->recv_in = mq->recv_out = 0;
    sys_stop_timer(mq->resp_tm);

    return OPRT_OK;
}

STATIC OPERATE_RET __mq_conn_recv(INOUT MQ_CNTL_S *mq)
{
    if(NULL == mq || (mq->status != MQTT_REC_MSG)) {
        return OPRT_MID_MQTT_INVALID_PARM;
    }

    BYTE_T *tmp_buf = NULL;
    BOOL_T mlk_buf = FALSE;
    OPERATE_RET op_ret = OPRT_OK;

    #define MQ_LEAST_DATA 2
#if OPERATING_SYSTEM <= SYSTEM_SMALL_MEMORY_END
    #define MAX_MQ_MESSAGE 4*1024
#else
    #define MAX_MQ_MESSAGE 256*1024*1024
#endif
    while((mq->recv_in-mq->recv_out) >= MQ_LEAST_DATA) {
        uint8_t fixed_head = mqtt_num_rem_len_bytes(mq->recv_buf+mq->recv_out);
        uint32_t remain_len = mqtt_parse_rem_len(mq->recv_buf+mq->recv_out);
        uint32_t msg_len = 1+fixed_head+remain_len;

        if(msg_len > MAX_MQ_MESSAGE) {
            op_ret = OPRT_MID_MQTT_OVER_MAX_MESSAGE_LEN;
            goto ERR_EXIT;
        }

        UINT_T recv_len = mq->recv_in-mq->recv_out;
        if(recv_len < msg_len) { // buf is not enough // <=
            tmp_buf = Malloc(msg_len+1);
            if(NULL == tmp_buf) {
                op_ret = OPRT_MID_MQTT_MALLOC_FAILED;
                goto ERR_EXIT;
            }
            memcpy(tmp_buf,(mq->recv_buf+mq->recv_out),recv_len);

            while(recv_len < msg_len) {
                UINT_T len = 0;
                op_ret = __mq_recv(mq,tmp_buf+recv_len,\
                                   (msg_len-recv_len),&len);
                if(op_ret != OPRT_OK) {
                    Free(tmp_buf);
                    op_ret = OPRT_MID_MQTT_DEF_ERR;
                    goto ERR_EXIT;
                }
                recv_len += len;
            }
            mlk_buf = TRUE;
        }else {
            tmp_buf = mq->recv_buf+mq->recv_out;
        }

        // 判断消息类型
        BYTE_T mq_msg_type;
        mq_msg_type = MQTTParseMessageType(tmp_buf);
        switch(mq_msg_type) {
            case MQTT_MSG_PUBLISH: {
                // qos process
                BYTE_T qos = MQTTParseMessageQos(tmp_buf);
                if(qos > 1) {
                    PR_ERR("do't support qos");
                    break;
                }else if(1 == qos) {
                    USHORT_T msg_id = mqtt_parse_msg_id(tmp_buf);
                    int ret = 0;
                    ret = mqtt_puback(&(mq->mq),msg_id);
                    if(ret < 0) {
                        PR_ERR("mqtt pub ack err");
                        break;
                    }
                }

                // message process
                uint8_t* ptr = NULL;
                uint32_t len = mqtt_parse_pub_topic_ptr(tmp_buf,(const uint8_t **)&ptr);
                if(0 == len){
                    continue;
                }
                UINT_T i;
                BOOL_T topic_match = FALSE;
                for(i=0; i<MAX_MQTT_TOPIC_NUM; i++){
                    if(0 == tuya_strncasecmp(mq->subcribe_topic[i],(CHAR_T *)ptr,len)){
                        topic_match = TRUE;
                        break;
                    }
                }
                if(!topic_match){
                    continue;
                }

                ptr = NULL;
                len = mqtt_parse_pub_msg_ptr(tmp_buf, (const uint8_t **)&ptr);
                if(0== len) {
                    continue;
                }
                uint8_t backup = ptr[len];
                ptr[len] = 0;
                if(mq->recv_cb) {
                    mq->recv_cb(ptr,len);
                }
                ptr[len] = backup;
            }
            break;

            case MQTT_MSG_PINGRESP: {
                /* 目前处理方式为当收到ping resp之后,逐步递减fail_cnt */
                mq->fail_cnt = mq->fail_cnt / 4;

                PR_DEBUG("ping respond. update fail_cnt:%d", mq->fail_cnt);
                sys_stop_timer(mq->resp_tm);

                module_start_tm_msg(mq->alive_tmm,(mq->mq.alive*1000),TIMER_ONCE);
            }
            break;

            case MQTT_MSG_PUBACK: { // qos == 1 process
                USHORT_T msg_id = mqtt_parse_msg_id(tmp_buf);
                __pub_ack_proc(mq,msg_id);
            }
            break;

            default: {
                goto ERR_EXIT;
            }
            break;
        }

        if(FALSE == mlk_buf) {
            mq->recv_out += msg_len;
        }else {
            mq->recv_out = mq->recv_in = 0;
            Free(tmp_buf);
            break;
        }
    }

    return OPRT_OK;

ERR_EXIT:
    mq->recv_out = mq->recv_in = 0;
    return op_ret;
}

STATIC VOID __mq_pre_conn(INOUT MQ_CNTL_S *mq)
{
    INT_T sleeptime = 0;
    INT_T heart_beat_lost = 0;
    
    tuya_hal_mutex_lock(mq->mutex);
    heart_beat_lost = mq->heart_beat_lost;
    if(heart_beat_lost > 0){
        mq->heart_beat_lost = 0;
    }
    tuya_hal_mutex_unlock(mq->mutex);

    //if heart beat lost, sleep before re-connect
    if(heart_beat_lost > 0){
        sleeptime = 1000 + tuya_hal_get_random_data(10 * 1000);
        PR_DEBUG("mqtt re-connect by heart beat lost, sleeptime:%d ms", sleeptime);
        tuya_hal_system_sleep(sleeptime);
    }
}

STATIC VOID __mq_ctrl_task(PVOID_T pArg)
{
    MQ_CNTL_S *mq = (MQ_CNTL_S *)pArg;
    OPERATE_RET op_ret = OPRT_OK;
    USHORT_T msg_id = 0;
    INT_T subscribe_index = 0;
    INT_T ret = 0;

    //PR_DEBUG("%s start",__FUNCTION__);

    while(GetThrdSta(mq->thread) == STATE_RUNNING)
    {
        // 连接之前，检查状态
        if(mq->permit_comm_cb) {
            INT_T result = mq->permit_comm_cb();
            
            // mqtt 不允许连接，就进入随机退避
            if(result == 0) {
                PR_ERR("mqtt not permit, random sleep..");
                goto MQ_RANDOM_DELAY;
            }
            
            // 网络状态异常，等待 s_permit_retry_interval (默认1000ms) 重试
            if(result == -1) {
                PR_TRACE("mqtt not permit. short sleep..");
                tuya_hal_system_sleep(s_permit_retry_interval);
                continue;
            }
        }

        switch(mq->status) {
            // 连接之前，获取服务器IP
            case MQTT_GET_SERVER_IP: {
                __mq_pre_conn(mq);
                mq->domain_index = 0;
                for(;mq->domain_index < mq->working_domain_num;mq->domain_index++) {
                    // 通过DNS解析域名
                    INT_T host_result = unw_gethostbyname(mq->working_domain_tbl[mq->domain_index],&mq->serv_ip);
                    if(UNW_SUCCESS == host_result) {
                        PR_DEBUG("select mqtt host:%s", mq->working_domain_tbl[mq->domain_index]);
                        break;
                    }else {
                        PR_ERR("resolve mqtt host Fail:%s", mq->working_domain_tbl[mq->domain_index]);
                        tuya_hal_system_sleep(1000);
                    }
                }

                // 解析失败，记录错误，并保持状态
                if(mq->domain_index >= mq->domain_num) {
                    mq->domain_index = 0;
                    __mqtt_log_seq_err(LOGSEQ_MQTT_DNS_RESV, OPRT_MID_MQTT_DNS_PARSED_FAILED, LDT_DEC);
                    continue;
                }

                // 解析成功，获取 IP 成功，状态切换为 MQTT_SOCK_CONN，并记录成功日志
                //INSERT_LOG_SEQ_DEC(LOGSEQ_MQTT_DNS_RESV, ret);
                if(mq->status != MQTT_QUIT_BEGIN)
                   mq->status = MQTT_SOCK_CONN;

                // 记录成功日志
                PR_NOTICE("mqtt get serve ip success");
            }
            break;

            // 开始连接socket
            case MQTT_SOCK_CONN: {
                __mq_pre_conn(mq);
                
                // 在每次连接socket前强行刷新usetable
                __mq_reset_use_table(mq);
                __set_mqtt_conn_stat(mq,FALSE);

                // 开始连接 socket
                op_ret = __mq_client_sock_conn(mq);
                if(OPRT_OK != op_ret) {
                    PR_ERR("mqtt connect fail. retry_cnt:%d ret:%d errno:%d" , mq->conn_retry_cnt, op_ret, tuya_hal_net_get_errno());
                    tuya_hal_system_sleep(1000);
                    mq->conn_retry_cnt++;

                    // 连接失败，重新进入请求服务器IP状态，并降低DNS级别，DNS级别降低的过程是不可逆的
                    if((mq->conn_retry_cnt > 1) || (UNW_ETIMEDOUT == tuya_hal_net_get_errno())) {
                        mq->conn_retry_cnt = 0;
                        if(mq->status != MQTT_QUIT_BEGIN) {
                            mq->status = MQTT_GET_SERVER_IP;
                            unm_lower_dns_cache_priority();
                        }
                    }
                    
                    // 只要mqtt socket 连接失败，记录上报错误信息，就进入随机退避
                    __mqtt_log_seq_err(LOGSEQ_MQTT_SOCK_CONN, op_ret, LDT_DEC);
                    goto MQ_EXIT_ERR;
                }

                // 连接成功，记录状态，并进入连接状态
                //INSERT_LOG_SEQ_DEC(LOGSEQ_MQTT_SOCK_CONN, op_ret);
                mq->conn_retry_cnt = 0;
                if(mq->status != MQTT_QUIT_BEGIN)
                    mq->status = MQTT_CONNECT;
                PR_NOTICE("mqtt create success, begin to connect");
            }
            break;

            // 连接、订阅状态
            case MQTT_CONNECT:
            case MQTT_SUBSCRIBE: {
                //INT_T seq_num = 0;

                // 启动定时器，监控应答超时
                sys_start_timer(mq->resp_tm,RESP_TIMEOUT*1000,TIMER_ONCE);
                if(MQTT_CONNECT == mq->status) {
                    // 连接状态
                    //seq_num = 12;

                    // 如果需要身份认证
                    if(mq->update_auth_cb) {
                        mq->update_auth_cb();
                    }

                    // 尝试连接mqtt服务器，状态设置为等待连接应答
                    ret = mqtt_connect(&mq->mq);
                    if(mq->status != MQTT_QUIT_BEGIN)
                        mq->status = MQ_CONN_RESP;

                    if (1 != ret) {
                        __mqtt_log_seq_err(LOGSEQ_MQTT_CONN, tuya_hal_net_get_errno(), LDT_DEC);
                        PR_ERR("ret:%d.", ret);
                        sys_stop_timer(mq->resp_tm);
                        goto MQ_EXIT_ERR;
                    }
                    
                    //INSERT_LOG_SEQ_DEC(LOGSEQ_MQTT_CONN, ret);
                } else {
                    // 订阅状态
                    if(subscribe_index >= MAX_MQTT_TOPIC_NUM)
                    {
                        PR_DEBUG("topic index out of range");
                        mq->status = MQTT_REC_MSG;                        
                        sys_stop_timer(mq->resp_tm);
                        break;
                    }

                    // 订阅topic非空，则开始订阅
                    if(mq->subcribe_topic[subscribe_index])
                    {
                        PR_NOTICE("mqtt connect success. begin to subscribe:%s",mq->subcribe_topic[subscribe_index]);
                        //seq_num = 13;

                        // 尝试订阅topic，状态设置为等待订阅应答
                        ret = mqtt_subscribe(&mq->mq,mq->subcribe_topic[subscribe_index], &msg_id); 
                        if(subscribe_index + 1 < MAX_MQTT_TOPIC_NUM && mq->subcribe_topic[subscribe_index + 1])
                        {
                            mq->status = MQ_SUB_RESP;
                            subscribe_index++;
                        }
                        else
                        {
                            // 没有额外的需要订阅的topic，设置状态为订阅完成
                            subscribe_index = 0;
                            mq->status = MQTT_SUB_FINISH;
                        }
                    }
                    else
                    {
                        // 没有需要订阅的topic，设置状态为订阅完成
                        PR_DEBUG("topic end");
                        subscribe_index = 0;
                        mq->status = MQTT_SUB_FINISH;                        
                        sys_stop_timer(mq->resp_tm);
                        break;
                    }
                    
                    // 连接或者订阅失败，记录错误并停止监听应答定时器
                    if(1 != ret) {
                        __mqtt_log_seq_err(LOGSEQ_MQTT_SUB, tuya_hal_net_get_errno(), LDT_DEC);
                        PR_ERR("ret:%d.", ret);
                        sys_stop_timer(mq->resp_tm);
                        goto MQ_EXIT_ERR;
                    }
                    
                    //INSERT_LOG_SEQ_DEC(LOGSEQ_MQTT_SUB, ret);
                }

                PR_ERR("ret:%d.",ret);
            }
            break;

            // 连接、订阅应答处理，订阅完成处理，接受信息处理
            case MQ_CONN_RESP:
            case MQ_SUB_RESP:
            case MQTT_SUB_FINISH:
            case MQTT_REC_MSG:
            {
                // 接受数据
                if(mq->recv_out && (mq->recv_in > mq->recv_out)) {
                    memcpy((mq->recv_buf), (mq->recv_buf+mq->recv_out), (mq->recv_in-mq->recv_out));
                    mq->recv_in = mq->recv_in-mq->recv_out;
                    mq->recv_out = 0;
                }else if(mq->recv_in <= mq->recv_out) {
                    mq->recv_in = 0;
                    mq->recv_out = 0;
                }

                UINT_T recv_len = 0;
                op_ret = __mq_recv(mq,(mq->recv_buf+mq->recv_in),\
                                   (DEF_MQ_RECV_BUF_LEN-mq->recv_in),&recv_len);
                if(op_ret != OPRT_OK || 0 == recv_len) {
                    PR_ERR("mq_recv err:%d len:%d",op_ret,recv_len);
                    __mqtt_log_seq_err(LOGSEQ_MQTT_RECV, op_ret, LDT_DEC);
                    goto MQ_EXIT_ERR;

                }
                mq->recv_in += recv_len;

                // 如果当前状态不是接受数据状态
                if(MQTT_REC_MSG != mq->status) {
                    op_ret = __mq_logic_conning_proc(mq,msg_id);
                    if(OPRT_OK != op_ret) {
                        PR_ERR("mq_logic_conn_proc err:%d",op_ret);
                        __mqtt_log_seq_err(LOGSEQ_MQTT_LOGIC_PROC, op_ret, LDT_DEC);
                        goto MQ_EXIT_ERR;
                    }
                    
                    //INSERT_LOG_SEQ_DEC(LOGSEQ_MQTT_LOGIC_PROC, op_ret);
                } else {
                    if(IsThisSysTimerRun(mq->resp_tm)) {
                        sys_stop_timer(mq->resp_tm);
                    }

                    // 接受数据，失败记录错误原因，成功不记录，因为太多了
                    op_ret = __mq_conn_recv(mq);
                    if(OPRT_OK != op_ret) {
                        PR_ERR("mq_conn_recv err:%d",op_ret);
                        INT_T errcode = (op_ret == OPRT_MID_MQTT_DEF_ERR) ? tuya_hal_net_get_errno() : op_ret;
                        __mqtt_log_seq_err(LOGSEQ_MQTT_CONN_RECV, errcode, LDT_DEC);
                        goto MQ_EXIT_ERR;
                    }
                }
            }
            break;
            // 开始退出
            case MQTT_QUIT_BEGIN: {
                PR_DEBUG("mqtt quit begin");
                goto MQ_EXIT_ERR;
            }
            break;
            // 退出完成
            case MQTT_QUIT_FINISH: {
                PR_ERR("mqtt quit finish");
                mq->status = MQTT_QUIT_BEGIN;
            }
            break;
        } // end of switch(mq->status)

        continue;

MQ_EXIT_ERR:
        unw_clear_all_dns_cache();
        PR_NOTICE("mqtt close");
        __mq_close(mq);
        subscribe_index = 0;

        if(mq->enable_wakeup == TRUE)
            continue;

MQ_RANDOM_DELAY:
        if(mq->status == MQTT_QUIT_BEGIN) {
            PR_DEBUG("quit mqtt while loop.");
            break;
        }

        mq->fail_cnt++;
        INT_T sleeptime = 1000 + tuya_hal_get_random_data(5000 + mq->fail_cnt * 1000);
        if(sleeptime > 5*60*1000)
            sleeptime = 5*60*1000;
        PR_DEBUG("mqtt fail_cnt:%u, sleeptime:%d ms", mq->fail_cnt, sleeptime);
        tuya_hal_system_sleep(sleeptime);
    }

    mq->status = MQTT_QUIT_FINISH;

    release_mqtt_hand(mq);
}

#if defined(ENABLE_IPC) && (LOW_POWER_ENABLE==1)
STATIC VOID __mq_ctrl_task_lp(PVOID_T  pArg)
{
    MQ_CNTL_S *mq = (MQ_CNTL_S *)pArg;
    OPERATE_RET op_ret = OPRT_OK;
    USHORT_T msg_id = 0;
    INT_T ret = 0;

    PR_DEBUG("%s start",__FUNCTION__);
    
    while(GetThrdSta(mq->thread) == STATE_RUNNING)
    {
        if(mq->permit_comm_cb) {
            INT_T result = mq->permit_comm_cb();
            if(result == 0) {
                PR_ERR("mqtt not permit, random sleep");
                //mqtt 不允许连接，就进入随机退避
                goto MQ_RANDOM_DELAY;
            }
            if(result == -1) {
                PR_ERR("mqtt not permit, short sleep");
                SystemSleep(1000);
                continue;
            }
        }

        // PR_DEBUG("mq->status:%d",mq->status);
        switch(mq->status) {
            case MQTT_GET_SERVER_IP: {
                __mq_pre_conn(mq);
                for(;mq->domain_index < mq->domain_num;mq->domain_index++) {
                    if(UNW_SUCCESS == unw_gethostbyname(mq->domain_tbl[mq->domain_index],&mq->serv_ip))
                    {
                        PR_DEBUG("select mqtt host:%s", mq->domain_tbl[mq->domain_index]);
                        break;
                    }else {
                        PR_ERR("resolve mqtt host Fail:%s", mq->domain_tbl[mq->domain_index]);
                        SystemSleep(1000);
                    }
                }

                if(mq->domain_index >= mq->domain_num) {
                    mq->domain_index = 0;
                    continue; //获取IP失败，一直获取
                }
                mq->status = MQTT_SOCK_CONN;
                PR_NOTICE("mqtt get serve ip success");
            }
            break;

            case MQTT_SOCK_CONN: {
                __mq_pre_conn(mq);
                /* 在每次连接socket前强行刷新usetable */
                __mq_reset_use_table(mq);

                __set_mqtt_conn_stat(mq,FALSE);
                op_ret = __mq_client_sock_conn(mq);
                if(OPRT_OK != op_ret) {
                    PR_ERR("mqtt connect fail. retry_cnt:%d ret:%d errno:%d" , mq->conn_retry_cnt, op_ret, unw_get_errno());
                    SystemSleep(1000);
                    mq->conn_retry_cnt++;
                    if((mq->conn_retry_cnt > 3) || (UNW_ETIMEDOUT == unw_get_errno())) {
                        mq->conn_retry_cnt = 0;
                        mq->status = MQTT_GET_SERVER_IP;
                    }
                    //只要mqtt socket 连接失败，就进入随机退避
                    goto MQ_EXIT_ERR;
                }
                mq->conn_retry_cnt = 0;
                mq->status = MQTT_CONNECT;
                PR_NOTICE("mqtt create success, begin to connect");
            }
            break;

            case MQTT_CONNECT:
            case MQTT_SUBSCRIBE: {
                sys_start_timer(mq->resp_tm,RESP_TIMEOUT*1000,TIMER_ONCE);
                if(MQTT_CONNECT == mq->status) {
                    if(mq->update_auth_cb) {
                        mq->update_auth_cb();
                    }
                    ret = mqtt_connect(&mq->mq);
                    mq->status = MQ_CONN_RESP;
                }else {
                    PR_NOTICE("mqtt connect success, begin to subscribe:%s",mq->subcribe_topic[0]);
                    ret = mqtt_subscribe(&mq->mq,mq->subcribe_topic[0], &msg_id);
                    // mq->status = MQ_SUB_RESP;
                    mq->status = MQTT_SUB_FINISH;
                }
                if(1 != ret) {
                    PR_ERR("ret:%d.",ret);
                    sys_stop_timer(mq->resp_tm);
                    goto MQ_EXIT_ERR;
                }
            }
            break;

            case MQ_CONN_RESP:
            case MQ_SUB_RESP:
            case MQTT_SUB_FINISH:
            case MQTT_REC_MSG:
            {
                if(mq->recv_out && (mq->recv_in > mq->recv_out))
                {
                    memcpy((mq->recv_buf), (mq->recv_buf+mq->recv_out), (mq->recv_in-mq->recv_out));
                    mq->recv_in = mq->recv_in-mq->recv_out;
                    mq->recv_out = 0;
                }else if(mq->recv_in <= mq->recv_out)
                {
                    mq->recv_in = 0;
                    mq->recv_out = 0;
                }

                UINT_T recv_len = 0;
                op_ret = __mq_recv(mq,(mq->recv_buf+mq->recv_in),\
                                   (DEF_MQ_RECV_BUF_LEN-mq->recv_in),&recv_len);
                if(op_ret != OPRT_OK || 0 == recv_len) {
                    PR_ERR("mq_recv err:%d len:%d",op_ret,recv_len);
                    goto MQ_EXIT_ERR;
                    
                }
                mq->recv_in += recv_len;

                if(MQTT_REC_MSG != mq->status) {
                    op_ret = __mq_logic_conning_proc(mq,msg_id);
                    if(OPRT_OK != op_ret) {
                        PR_ERR("mq_logic_conn_proc err:%d",op_ret);
                        goto MQ_EXIT_ERR;
                    }else if (TRUE == get_mqtt_conn_stat((MQ_HANDLE)mq)){
                        PR_DEBUG("MQTT sub succuss");
                        return ;
                    }
                }else {
                    if(IsThisSysTimerRun(mq->resp_tm)) { 
                        sys_stop_timer(mq->resp_tm);
                    }

                    op_ret = __mq_conn_recv(mq);
                    if(OPRT_OK != op_ret) {
                        PR_ERR("mq_conn_recv err:%d",op_ret);
                        goto MQ_EXIT_ERR;
                    }
                }
            }
            break;
        } // end of switch(mq->status)

        continue;
        
MQ_EXIT_ERR:
        unw_clear_all_dns_cache();
        __mq_close(mq);

MQ_RANDOM_DELAY:
        mq->fail_cnt++;
        INT_T sleeptime = 1000 + rand() % (5000 + mq->fail_cnt * 1000);
        if(sleeptime > 5*60*1000)
            sleeptime = 5*60*1000;
        PR_DEBUG("mqtt fail_cnt:%u,sleeptime:%d ms", mq->fail_cnt, sleeptime);
        SystemSleep(sleeptime);
    }
}
#endif

STATIC VOID __pub_ack_proc(INOUT MQ_CNTL_S *mq,IN CONST USHORT_T msg_id)
{
    MQ_PUB_ASYNC_S *mpa = &(mq->mpa);

    INT_T i;
    tuya_hal_mutex_lock(mq->data_mutex);
    for(i = 0;i < CONC_MQM_NUM;i++) {
        if(0 == ((mpa->b_use[i/8] >> i%8) & 0x01)) {
            continue;
        }

        if(mpa->mqmde_tbl[i].msg_id == msg_id) {
            if(mpa->mqmde_tbl[i].cb) {
                mpa->mqmde_tbl[i].cb(OPRT_OK,mpa->mqmde_tbl[i].prv_data);
            }
            mpa->b_use[i/8] &= ~(1 << i%8);

            mq->send_timeout_cnt = 0;
            tuya_hal_mutex_unlock(mq->data_mutex);
            return;
        }
    }
    tuya_hal_mutex_unlock(mq->data_mutex);
}

STATIC VOID __pub_ack_timeout_proc(INOUT MQ_CNTL_S *mq)
{
    MQ_PUB_ASYNC_S *mpa = &(mq->mpa);
	/*uni_time_get_posix need time synchronization, time would jump after synchronized.*/
    TIME_S t = 0;
    uni_get_system_time(&t,NULL);

    INT_T i;
    tuya_hal_mutex_lock(mq->data_mutex);
    for(i = 0;i < CONC_MQM_NUM;i++) {
        if(0 == ((mpa->b_use[i/8] >> i%8) & 0x01)) {
            continue;
        }

        if((t - mpa->mqmde_tbl[i].time) >= mpa->mqmde_tbl[i].to_lmt) {
            PR_DEBUG("qos1 package timeout, cnt:%d", mq->send_timeout_cnt);
            if(mpa->mqmde_tbl[i].cb) {
                mpa->mqmde_tbl[i].cb(OPRT_MID_MQTT_PUBLISH_TIMEOUT,mpa->mqmde_tbl[i].prv_data);
            }
            mpa->b_use[i/8] &= ~(1 << i%8);

            if(++mq->send_timeout_cnt >= SND_TO_CMT_LMT) {
                mq->send_timeout_cnt = 0;
                PR_NOTICE("cnt>3, mq close");

                __mqtt_log_seq_err(LOGSEQ_MQTT_PUB_ACK_CLOSE, uni_time_get_posix(), LDT_TIMESTAMP);
                __mq_close(mq);
            } else {
                //reset all timeout
                INT_T j;
                for(j = 0; j < CONC_MQM_NUM; j++) {
                    if(0 == ((mpa->b_use[j/8] >> j%8) & 0x01)) {
                        continue;
                    }

                    if(mpa->mqmde_tbl[j].time < t) {
                        mpa->mqmde_tbl[j].time = t;
                        PR_NOTICE("reset mid %d", mpa->mqmde_tbl[j].msg_id);
                    }
                }
            }
            tuya_hal_mutex_unlock(mq->data_mutex);
            return;
        }
    }
    tuya_hal_mutex_unlock(mq->data_mutex);
}

/***********************************************************
*  Function: mqtt_publish_async
*  Input: hand
*         topic
*         qos if(0 == qos) then to_lmt cb prv_data useless,
*                 becase no respond wait.
*             else if(1 == qos) then need wait respond.
*             else then do't support.
*         data
*         len
*         to_lmt timeout limit if(0 == to_lmt) then use system default limit
*         cb
*         prv_data
*  Output: hand
*  Return: OPERATE_RET
***********************************************************/
OPERATE_RET mqtt_publish_async(IN CONST MQ_HANDLE hand,IN CONST CHAR_T *topic,IN CONST BYTE_T qos,\
                                        IN CONST BYTE_T *data,IN CONST INT_T len,IN CONST UINT_T to_lmt,\
                                        IN CONST MQ_PUB_ASYNC_IFM_CB cb,IN VOID *prv_data)
{
    if((NULL == hand) || (NULL == data) || (0 == len) || (qos > 1)) {
        return OPRT_INVALID_PARM;
    }

    INT_T ret = 0;
    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;

    if(0 == qos) {
        ret = mqtt_pub_msg_with_qos(&mq->mq,topic,data,len,0,qos, 0);
        if(ret < 0) {
            PR_ERR("mqtt_pub_msg_with_qos err:%d",ret);
            return OPRT_COM_ERROR;
        }
    }else {
        MQ_MSG_DAT_ELE_S *mqmde = NULL;
        INT_T i = 0;
        MQ_PUB_ASYNC_S *mpa = &(mq->mpa);
        USHORT_T msg_id = 0;

        tuya_hal_mutex_lock(mq->data_mutex);

        if(cb) {
            for(i = 0;i < CONC_MQM_NUM;i++) {
                if(0 == ((mpa->b_use[i/8] >> i%8) & 0x01)) {
                    break;
                }
            }

            if(i >= CONC_MQM_NUM) {
                PR_ERR("can't find free mqmde");
                tuya_hal_mutex_unlock(mq->data_mutex);
                return OPRT_COM_ERROR;
            }

            mqmde = &(mq->mpa.mqmde_tbl[i]);
            mqmde->cb = cb;
            mqmde->prv_data = prv_data;
            // mqmde->index = ret;
            uni_get_system_time(&mqmde->time,NULL);
            if(0 == to_lmt) {
                mqmde->to_lmt = PUB_RESP_TO_LMT;
            }else {
                mqmde->to_lmt = to_lmt;
            }

            mqtt_get_qos_msg_id(&mq->mq, qos, &msg_id);

            mpa->b_use[i/8] |= (1 << i%8);
            mqmde->msg_id = msg_id;
        }else {
            mqtt_get_qos_msg_id(&mq->mq, qos, &msg_id);
        }
        tuya_hal_mutex_unlock(mq->data_mutex);

        ret = mqtt_pub_msg_with_qos(&mq->mq,topic,data,len,0,qos,msg_id);
        if(ret < 0) {
            if(cb) {
                  mpa->b_use[i/8] &= ~(1 << i%8);
            }

            __mqtt_log_seq_err(LOGSEQ_MQTT_PUB, tuya_hal_net_get_errno(), LDT_DEC);
            PR_ERR("pub_msg_with_qos err:%d errno:%d",ret,errno);
            return ret;
        }
    }

    return OPRT_OK;
}

/***********************************************************
*  Function: mqtt_set_permit_retry_interval
*  Input: retry_interval in ms when permit cb return false
*  Output: none
*  Return: none
***********************************************************/
VOID mqtt_set_permit_retry_interval(IN CONST UINT_T retry_interval)
{
    s_permit_retry_interval = retry_interval;
}

INT_T mqtt_get_socket_fd(IN CONST MQ_HANDLE hand)
{
   if(NULL == hand)
   {
       PR_ERR("hand nul");
       return -1;
   }

   MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
   return mq->fd;
}

INT_T mqtt_get_alive_time_s(IN CONST MQ_HANDLE hand)
{
    if(NULL == hand)
    {
        PR_ERR("hand nul");
        return -1;
    }

    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    return mq->mq.alive;
}

OPERATE_RET mqtt_book_wakeup_topic(IN CONST MQ_HANDLE hand, IN CONST CHAR_T *wakeup_topic)
{
    INT_T ret;
    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    USHORT_T msg_id = 0;
    if(NULL == hand)
    {
        PR_ERR("hand nul");
        return OPRT_COM_ERROR;
    }

    mq->enable_wakeup = TRUE;
    __mq_close(hand);

    INT_T index = 0;
    for(index = 0; index < 100; index++) {
        tuya_hal_system_sleep(100);
        if(get_mqtt_conn_stat(hand) == TRUE)
            break;
        PR_DEBUG("wait mqtt reconnect..%d", index);
    }
    if(get_mqtt_conn_stat(hand) != TRUE) {
        PR_ERR("reconnect fail.");
        return OPRT_COM_ERROR;
    }
    PR_DEBUG("wait mqtt reconnect success..%d", index);

    PR_DEBUG("unscribe:%s", mq->subcribe_topic);
    ret = mqtt_unsubscribe(&mq->mq, (const char*)mq->subcribe_topic, &msg_id);
    if(ret != 1)
    {
        PR_ERR("unsubscribe:%s err:%d", mq->subcribe_topic, ret);
    }

    ret = mqtt_subscribe(&mq->mq, wakeup_topic, &msg_id);
    PR_DEBUG("subscribe:%s ret:%d", wakeup_topic, ret);

    return (ret == 1) ? OPRT_OK : OPRT_COM_ERROR;
}

OPERATE_RET mqtt_book_additional_topic(IN CONST MQ_HANDLE hand, IN CONST CHAR_T *topic)
{
    INT_T ret = 0;
    MQ_CNTL_S *mq = (MQ_CNTL_S *)hand;
    USHORT_T msg_id = 0;
    if(NULL == hand)
    {
        PR_ERR("hand nul");
        return OPRT_COM_ERROR;
    }

    UINT_T i;
    for (i=0; i<MAX_MQTT_TOPIC_NUM; i++)
    {
        if(mq->subcribe_topic[i] == NULL)
        {
            mq->subcribe_topic[i] = Malloc(strlen(topic)+1);
            if(mq->subcribe_topic[i])
            {
                strcpy(mq->subcribe_topic[i],topic);
                ret = mqtt_subscribe(&mq->mq, topic, &msg_id);
                PR_DEBUG("subscribe new topic %s ret:%d ", topic, ret);
            }
        }
    }
    return (ret == 1) ? OPRT_OK : OPRT_COM_ERROR;
}

