#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <poll.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <systemd/sd-daemon.h>
#include <mysql.h>

#define COMMAND_LEN(CMD_NAME) (sizeof(struct CMD_NAME) - 4)

const char * faction_str[] = {"N/A", "HSD", "PRF", "RB", "JAR", "ERR"};
static inline const char * get_faction(uint8_t faction_id) {
    if (faction_id > 5) faction_id = 5;
    return faction_str[faction_id];
}

struct command_base {
    uint16_t len;
    uint16_t code;
} __attribute__((packed));

struct command_status {
    uint16_t len;
    uint16_t code;
    uint32_t arg;
} __attribute__((packed));

struct command_xuid {
    uint16_t len;
    uint16_t code;
    uint64_t xuid;
} __attribute__((packed));

struct command_turn_days {
    uint16_t len;
    uint16_t code;
    uint16_t days;
} __attribute__((packed));

struct resp_status {
    uint16_t len;
    uint16_t code;
    uint8_t status;
} __attribute__((packed));

struct resp_period {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    int32_t seconds;
} __attribute__((packed));

struct vt_potential_cost_entry {
    uint16_t type;
    uint16_t cost;
} __attribute__((packed));

struct resp_vt_potential_cost {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint16_t costs_len;
    struct vt_potential_cost_entry costs[];
} __attribute__((packed));

struct req_freemsg {
    uint16_t len;
    uint16_t code;
    uint8_t language;
} __attribute__((packed));

struct resp_freemsg {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint8_t padding;
    uint16_t msg_len;
    uint16_t msg[];
} __attribute__((packed));

struct resp_fixedmsg {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint16_t msg_len;
    uint8_t msg[16];
} __attribute__((packed));

struct req_round_turn {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
} __attribute__((packed));

struct req_round_turn_map {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t map;
} __attribute__((packed));

struct resp_war_influence {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    int32_t padding;
    // These 3 values must add up to 100
    int32_t hsd;
    int32_t prf;
    int32_t rb;
} __attribute__((packed));

struct resp_war_tide {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint8_t padding[5];
    // These 3 values must add up to 100
    int32_t hsd;
    int32_t prf;
    int32_t rb;
} __attribute__((packed));

#define PILOT_NAME_LEN 16
#define PILOT_NAME_OFFSET 40
#define PILOT_DATA_SIZE 1816

struct resp_get_user_info {
    uint16_t len;
    uint16_t code;
    uint8_t status; // 0=Registered, 1=Unregistered
    uint8_t name[PILOT_NAME_LEN];
    uint8_t faction;
    uint16_t data_size;
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct req_new_user_info {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    uint8_t xname[PILOT_NAME_LEN];
    uint8_t faction;
    uint16_t data_size;
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct req_set_user_info {
    uint16_t len;
    uint16_t code;
    uint16_t data_size;
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct vt_entry {
    uint16_t type;
    uint32_t serial;
} __attribute__((packed));

struct vt_list {
    uint16_t len;
    struct vt_entry list[];
} __attribute__((packed));

struct resp_vt_list {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    struct vt_list vts;
} __attribute__((packed));

struct req_vt_list {
    uint16_t len;
    uint16_t code;
    struct vt_list vts;
} __attribute__((packed));

struct shop_entry {
    uint16_t type;
    uint32_t limit;
    uint16_t cost;
    uint8_t padding;
} __attribute__((packed));

struct resp_shop_list {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint16_t list_len;
    struct shop_entry list[];
} __attribute__((packed));

struct req_vt_add {
    uint16_t len;
    uint16_t code;
    uint16_t list_len;
    uint16_t list[];
} __attribute__((packed));

struct req_set_user_faction {
    uint16_t len;
    uint16_t code;
    uint8_t faction;
} __attribute__((packed));

struct req_vt_capture {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    struct vt_list vts;
} __attribute__((packed));

struct resp_vt_capture {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint8_t name[PILOT_NAME_LEN];
    struct vt_list vts;
} __attribute__((packed));

struct req_analyze {
    uint16_t len;
    uint16_t code;
    uint32_t data[10];
} __attribute__((packed));

struct req_message_count {
    uint16_t len;
    uint16_t code;
    uint8_t a;
    uint8_t b;
} __attribute__((packed));

struct resp_message_count {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint16_t padding;
    uint32_t start;
    uint16_t count;
} __attribute__((packed));

struct resp_message_post {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint16_t padding;
    uint32_t id;
} __attribute__((packed));

struct req_add_user_point {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t map;
    uint32_t points[10];
} __attribute__((packed));

struct req_add_mission_point {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t map;
    uint32_t points;
    uint8_t faction;
} __attribute__((packed));

struct req_round_points {
    uint16_t len;
    uint16_t code;
    uint16_t round;
} __attribute__((packed));

struct resp_round_points {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint32_t points;
    uint8_t hsd;
    uint8_t prf;
    uint8_t rb;
} __attribute__((packed));

struct resp_rank_count_categories {  
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint32_t padding;
    uint32_t count[8];
} __attribute__((packed));

struct req_rank_count_global {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t category;
} __attribute__((packed));

struct req_rank_count_faction {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t category;
    uint8_t faction;
} __attribute__((packed));

struct resp_rank_count {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint8_t padding0;
    uint32_t padding1;
    union {
        struct {
            uint32_t count;
            uint32_t padding2;
        } pt;
        struct {
            uint32_t place;
            uint32_t count;
        } loc;
    };
    uint32_t unknown;
} __attribute__((packed));

struct req_rank_values {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t faction;
    uint8_t category;
    uint32_t start;
    uint16_t count;
} __attribute__((packed));

struct rank_entry {
    uint32_t rank;
    uint8_t name[PILOT_NAME_LEN];
    uint8_t xname[PILOT_NAME_LEN];
    uint32_t a;
    uint32_t score;
} __attribute__((packed));

struct rank_list {
    uint16_t len;
    struct rank_entry list[];
} __attribute__((packed));

struct resp_rank_values {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint8_t padding[6];
    uint32_t a;
    struct rank_list ranks;
} __attribute__((packed));

struct award_entry {
    uint16_t round;
    uint16_t turn;
    uint32_t a;
    uint16_t b;
    uint32_t c;
} __attribute__((packed));

struct award_list {
    uint16_t len;
    struct award_entry list[];
} __attribute__((packed));

struct resp_award_available {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    struct award_list awards;
} __attribute__((packed));

struct req_trade_token {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
} __attribute__((packed));

struct resp_trade_token {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint32_t token;
} __attribute__((packed));

struct req_trade_user_info {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    uint32_t token;
    uint16_t data_size;
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct resp_trade_user_info {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    uint16_t delay;
} __attribute__((packed));

struct req_trade_vt {
    uint16_t len;
    uint16_t code;
    uint32_t token;
    uint8_t name[PILOT_NAME_LEN];
    // Both of these have variable length so decode them dynamically
//    struct vt_list rx_vts;
//    struct vt_list tx_vts;
} __attribute__((packed));

struct loc_mystery_entry {
    uint16_t a;
    uint16_t b;
    uint32_t c; // Unused?
    uint16_t d;
    uint32_t e;
    uint8_t f; // Faction?
} __attribute__((packed)); // 15 bytes

struct loc_mystery_list {
    uint16_t len; // Should not exceed 128
    struct loc_mystery_entry list[];
} __attribute__((packed));

struct resp_loc_mystery {
    uint16_t len;
    uint16_t code;
    uint8_t status;
    struct loc_mystery_list values;
} __attribute__((packed));

struct command_base cmd_liveness = {COMMAND_LEN(command_base), 0x1010};
struct command_base cmd_xuidreq = {COMMAND_LEN(command_base), 0x1020};
struct command_status cmd_connect = {COMMAND_LEN(command_status), 0x3013};
struct command_status cmd_reject = {COMMAND_LEN(command_status), 0x3014};
struct command_status cmd_wait = {COMMAND_LEN(command_status), 0x3015};

#define TURN_DAYS 7
#define DAY_SECONDS 86400
#define TURN_SECONDS (TURN_DAYS * DAY_SECONDS)
#define NUMBER_OF_MAPS 24

struct command_turn_days cmd_turn_days = {COMMAND_LEN(command_turn_days), 0x4030, TURN_DAYS};

#define RECV_BUF_LEN 5124
#define MAX_CMD_LEN 2048 // Never seen a request from the client larger than this
#define MAX_CONFIG_LINE_LEN 32

enum GameType {
    UnknownGame,
    Brainbox,
    PilotTest,
    LineOfContact
};

char * GameType_string[] = {
    "Unknown GameType",
    "Brainbox",
    "Pilot Test",
    "Line of Contact"
};

struct config_data {
    enum GameType game;
    struct sockaddr_in server_addr;
    char user[MAX_CONFIG_LINE_LEN];
    char pass[MAX_CONFIG_LINE_LEN];
    char schema[MAX_CONFIG_LINE_LEN];
} config;

struct client_data {
    int tid;
    int client_fd;
    MYSQL * sqlcon;
    
    uint64_t xuid;
    char xname[PILOT_NAME_LEN];

    uint8_t faction;
    int8_t language;
    uint8_t banned;
    
    uint16_t recvbuf_len;
    uint8_t recvbuf[RECV_BUF_LEN];
};

ssize_t client_count;
pthread_mutex_t client_mtx;

static __thread struct client_data * local_data;

enum trade_state {
    trade_state_idle,
    trade_state_waiting,
    trade_state_complete
};

#define TRADE_COUNT 1024
struct trade_data {
    uint32_t token;
    enum trade_state state;
    uint16_t code;
} trades[TRADE_COUNT];

uint32_t current_token;
pthread_mutex_t trade_mtx;

void logmsg(const char * format, ...) {
    char msg[1024];
    va_list args;
    va_start(args, format);
    vsnprintf(msg, sizeof(msg), format, args);
    va_end(args);
    
    printf("[%-10d][%016lX|%-16.16s]%s\n", local_data->tid, local_data->xuid, local_data->xname, msg);
}

void client_handler_exit(long retval) {
    // Report an error to the XBOX
    if (retval) write(local_data->client_fd, &cmd_reject, sizeof(cmd_reject));
    
    pthread_mutex_lock(&client_mtx);
    // Decrement thread count
    client_count--;
    logmsg("Client hangup: Exit Code %ld, Remaining clients %d", retval, client_count);
    pthread_mutex_unlock(&client_mtx);
    
    // Close the connection
    close(local_data->client_fd);

    // Close DB connection if it was open
    if (local_data->sqlcon) mysql_close(local_data->sqlcon);

    free(local_data);
    
    mysql_thread_end();
    
    pthread_exit((void *)retval);
}

void sendcmd(void * raw_cmd) {
    struct command_base * cmd = raw_cmd;
    write(local_data->client_fd, cmd, cmd->len + sizeof(struct command_base));
}

void * recvcmd(void) {
    struct command_base * cmd = (struct command_base *)local_data->recvbuf;
    uint16_t command_len;

    // Cleanup last receive command
    if (local_data->recvbuf_len) {
        uint16_t nextmsg = sizeof(struct command_base) + cmd->len;
        if (nextmsg > local_data->recvbuf_len) {
            logmsg("nextmsg offset (%d) is greater than recvbuf_len (%d)", nextmsg, local_data->recvbuf_len);
            client_handler_exit(1);
        }
        
        local_data->recvbuf_len -= nextmsg;
        memmove(local_data->recvbuf, local_data->recvbuf + nextmsg, local_data->recvbuf_len);
    }

    do {
        // Setup FD watch list
        struct pollfd recv_fd = {local_data->client_fd, POLLIN};
        int wait_ret = poll(&recv_fd, 1, 9 * 1000);
        if (wait_ret < 0) {
            char errmsg[256];
            strerror_r(errno, errmsg, 256);

            logmsg("poll() failed, got error: %s", errmsg);
            client_handler_exit(1);
        } else if (!wait_ret) {
            logmsg("Receive timeout");
            client_handler_exit(1);
        }
        
        ssize_t read_ret = read(local_data->client_fd, local_data->recvbuf + local_data->recvbuf_len, RECV_BUF_LEN - local_data->recvbuf_len);
        if (!read_ret) {
            return NULL;
//            logmsg("read() failed, socket prematurely closed");
//            client_handler_exit(1);
        } else if (read_ret < 0) {
            char errmsg[256];
            strerror_r(errno, errmsg, 256);
            
            logmsg("read() failed, got error: %s", errmsg);
            client_handler_exit(1);
        }
        
        local_data->recvbuf_len += read_ret;
        
        // Keep going until we've receive a full command        
        if (local_data->recvbuf_len < sizeof(struct command_base)) continue;
        
        command_len = cmd->len + sizeof(struct command_base);
        
        // Fail on null commands
        if (!cmd->len && !cmd->code) {
            logmsg("Null command in recvbuffer");
            client_handler_exit(1);
        }
        
        // Sanity check command length
        if (command_len > RECV_BUF_LEN) {
            logmsg("Request was too long %d", command_len);
            client_handler_exit(1);
        }
    } while (local_data->recvbuf_len < command_len); // Make sure we have all the data
    
    return cmd;
}

void handle_turn_days(void) {
    logmsg("Request Turn Days");
    
    char query[256];
    snprintf(query, sizeof(query),
        "UPDATE garage SET sortied = 0 WHERE xuid = CONV('%016lX', 16, 10);",
        local_data->xuid);
    
    // Recover sortied VTs from a disconnected match
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to update garage while recovering sortied VTs, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    sendcmd(&cmd_turn_days);
    logmsg("Response Turn Days: %d Days", cmd_turn_days.days);
}

void handle_loc_unknown(void) {
    logmsg("Request LOC mystery command");
    
    size_t rowcount = 0;
    struct resp_loc_mystery * loc_mystery = malloc(sizeof(struct resp_loc_mystery) + sizeof(struct loc_mystery_entry) * rowcount);
    loc_mystery->code = 0x8041;
    loc_mystery->status = 0;
    loc_mystery->values.len = rowcount;
    loc_mystery->len = COMMAND_LEN(resp_loc_mystery) + sizeof(struct loc_mystery_entry) * rowcount;

    sendcmd(loc_mystery);
    free(loc_mystery);
}

void handle_campaign_period(void) {
    logmsg("Request campaign period");
    
    if (mysql_query(local_data->sqlcon, "SELECT round, turn, (UNIX_TIMESTAMP(period) - UNIX_TIMESTAMP(NOW())) FROM settings ORDER BY round DESC, turn DESC LIMIT 1;")) {
        logmsg("Failed to query campaign period, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get campaign period, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 3) {
        logmsg("Incorrect number of fields (%d) in campaign period", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row || !row[0] || !row[1] || !row[2]) {
        logmsg("Failed to get row from campaign period, got error: %s", mysql_error(local_data->sqlcon));
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    struct resp_period period = {
        COMMAND_LEN(resp_period),
        0x2030,
        strtoul(row[0], NULL, 10),
        strtoul(row[1], NULL, 10),
        strtoul(row[2], NULL, 10)
    };
    mysql_free_result(sqlres);
    
    if (!period.round) {
        logmsg("Invalid campaign Round");
        client_handler_exit(1);
    }

    if (period.turn > 8) {
        logmsg("Invalid campaign Turn %d", period.turn);
        client_handler_exit(1);        
    }

    if (period.seconds < 0) {
        logmsg("Seconds left in Turn was negative (%d)", period.seconds);
        client_handler_exit(1);
    }
    
    period.seconds += (8 - period.turn) * TURN_SECONDS;
    
    logmsg("Campaign period response: Round %d, Turn %d, Seconds %d", period.round, period.turn, period.seconds);
    
    sendcmd(&period);
}

void handle_vt_potential_cost(void) {
    logmsg("Request VT potential cost");
    
    /*
    if (mysql_query(local_data->sqlcon, "SELECT type, vtpts FROM shop WHERE vtpts IS NOT NULL;")) {
        logmsg("Failed to query VT potential cost, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get VT potential cost, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 2) {
        logmsg("Incorrect number of fields (%d) in VT potential cost", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    size_t rowcount = mysql_num_rows(sqlres);
    */
    size_t rowcount = 28;
    struct resp_vt_potential_cost * potentialcost = malloc(sizeof(struct resp_vt_potential_cost) + sizeof(struct vt_potential_cost_entry) * rowcount);
    potentialcost->code = 0x2049;
    potentialcost->status = 0;
    potentialcost->costs_len = rowcount;
    potentialcost->len = COMMAND_LEN(resp_vt_potential_cost) + sizeof(struct vt_potential_cost_entry) * rowcount;
        
    for (int i = 0; i < rowcount; i++) {
//        MYSQL_ROW row = mysql_fetch_row(sqlres);
        
        potentialcost->costs[i].type = i;//strtoul(row[0], NULL, 10);
        potentialcost->costs[i].cost = 1;//strtoul(row[1], NULL, 10);
    }

//    mysql_free_result(sqlres);
    
    logmsg("Potential cost response, %d entries", rowcount);
    
    sendcmd(potentialcost);
    free(potentialcost); // Cleanup
}

void handle_war_influence(void) {
    struct req_round_turn * war_influence_in = (struct req_round_turn *)local_data->recvbuf;
    logmsg("Request War Influence: Round %d, Turn %d", war_influence_in->round, war_influence_in->turn);
    
    char query[256];
    if (war_influence_in->round) {
        snprintf(query, sizeof(query), "SELECT hsd, prf, rb FROM wartide WHERE round = %d;", war_influence_in->round);
    } else {
        strncpy(query, "SELECT hsd, prf, rb FROM wartide WHERE (round) IN (SELECT MAX(round) FROM wartide);", sizeof(query));
    }

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query war influence message, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get war influence, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int rb_exist = war_influence_in->turn >= 3;
    
    double hsd = 0.0;
    double prf = 0.0;
    double rb = 0.0;
    
    uint64_t rowcount = mysql_num_rows(sqlres);
    for (int i = 0; i < rowcount; i++) {
        MYSQL_ROW row = mysql_fetch_row(sqlres);
        int hsd_map = strtoul(row[0], NULL, 10);
        int prf_map = strtoul(row[1], NULL, 10);
        int rb_map  = strtoul(row[2], NULL, 10);
        
        if (hsd_map > prf_map && hsd_map > rb_map) {
            hsd++;
        } else if (prf_map > hsd_map && prf_map > rb_map) {
            prf++;
        } else if (rb_map > prf_map && rb_map > hsd_map) {
            rb++;
        }
    }
    mysql_free_result(sqlres);
    
    double unplayed = rowcount < NUMBER_OF_MAPS ? NUMBER_OF_MAPS - rowcount : 0.0;
    if (rb_exist) {
        unplayed /= 3.0;
        hsd += unplayed;
        prf += unplayed;
        rb  += unplayed;
    } else {
        unplayed /= 2.0;
        hsd += unplayed;
        prf += unplayed;
    }
    
    double total = hsd + prf + rb;
    // Prevent divide by zero
    if (!(int)total) total++;
    
    int32_t factions[3] = {
        hsd / total * 100.0,
        prf / total * 100.0,
        rb  / total * 100.0
    };
    
    // Make sure all 3 values sum to 100
    int32_t diff = 100 - (factions[0] + factions[1] + factions[2]);

    factions[rb_exist ? 2 : 1] += diff;

    struct resp_war_influence war_influence = {COMMAND_LEN(resp_war_influence), 0x2032};
    war_influence.hsd = factions[0];
    war_influence.prf = factions[1];
    war_influence.rb  = factions[2];
    
    sendcmd(&war_influence);
    logmsg("War influence response: HSD: %d, PRF: %d, RB: %d, Diff: %d",
        factions[0], factions[1], factions[2], diff);
}

void handle_war_tide(void) {
    struct req_round_turn_map * war_tide_in = (struct req_round_turn_map *)local_data->recvbuf;
    logmsg("Request War Tide: Round %d, Turn %d, Map %d", war_tide_in->round, war_tide_in->turn, war_tide_in->map);
    
    int map_id = war_tide_in->map + (war_tide_in->turn - 1) * 3;
    
    char query[256];
    snprintf(query, sizeof(query),
        "SELECT hsd, prf, rb FROM wartide WHERE round = %d AND map = %d;",
        war_tide_in->round, map_id);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query war tide, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get war tide, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 3) {
        logmsg("Incorrect number of fields (%d) in war tide", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }

    int rb_exist = war_tide_in->turn >= 3;

    // Give each faction some starting points so that early victories dont skew the pie-chart
    double hsd = 300.0;
    double prf = 300.0;
    double rb  = 300.0;
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (row) {
        hsd += strtoul(row[0], NULL, 10);
        prf += strtoul(row[1], NULL, 10);
        rb  += strtoul(row[2], NULL, 10);
    }
    mysql_free_result(sqlres);
    
    if (!rb_exist) rb = 0.0;
    
    double total = hsd + prf + rb;
    // Prevent divide by zero
    if (!(int)total) total++;
    
    int32_t factions[3] = {
        hsd / total * 100.0,
        prf / total * 100.0,
        rb  / total * 100.0
    };
    
    // Make sure all 3 values sum to 100
    int32_t diff = 100 - (factions[0] + factions[1] + factions[2]);

    factions[rb_exist ? 2 : 1] += diff;

    struct resp_war_tide war_tide = {COMMAND_LEN(resp_war_tide), 0x2031};
    war_tide.hsd = factions[0];
    war_tide.prf = factions[1];
    war_tide.rb  = factions[2];

    sendcmd(&war_tide);    
    logmsg("War tide response: HSD: %d, PRF: %d, RB: %d, Diff: %d",
        factions[0], factions[1], factions[2], diff);
}

void handle_round_points(void) {
    struct req_round_points * round_points_in = (struct req_round_points *)local_data->recvbuf;
    logmsg("Request end-of-round points: Round %d", round_points_in->round);
    
    char query[256];
    snprintf(query, sizeof(query), "SELECT hsd, prf, rb FROM wartide WHERE round = %d;", round_points_in->round);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query battle status for round points, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get battle status for round points, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int hsd = 0;
    int prf = 0;
    int rb = 0;
    
    uint64_t rowcount = mysql_num_rows(sqlres);
    for (int i = 0; i < rowcount; i++) {
        MYSQL_ROW row = mysql_fetch_row(sqlres);
        int hsd_map = strtoul(row[0], NULL, 10);
        int prf_map = strtoul(row[1], NULL, 10);
        int rb_map  = strtoul(row[2], NULL, 10);
        
        if (hsd_map > prf_map && hsd_map > rb_map) {
            hsd++;
        } else if (prf_map > hsd_map && prf_map > rb_map) {
            prf++;
        } else if (rb_map > prf_map && rb_map > hsd_map) {
            rb++;
        }
    }
    mysql_free_result(sqlres);
    
    struct resp_round_points round_points = {COMMAND_LEN(resp_round_points), 0x2028};
    // TODO: Fetch this dynamically from the DB
    /*
        0 = Annihilated
        1 = 1st
        2 = 2nd
        3 = 3rd
    */
    round_points.points = 100000;
    round_points.hsd = 3;
    round_points.prf = 3;
    round_points.rb  = 3;
    
    if (hsd) {
        if (hsd >= prf) round_points.hsd--;
        if (hsd >= rb)  round_points.hsd--;
    } else round_points.hsd = 0;

    if (prf) {
        if (prf >= hsd) round_points.prf--;
        if (prf >= rb)  round_points.prf--;
    } else round_points.prf = 0;
    
    if (rb) {
        if (rb >= hsd) round_points.rb--;
        if (rb >= prf) round_points.rb--;
    } else round_points.rb = 0;
    
    logmsg("Round points response: %d Points, HSD: %d, PRF: %d, RB: %d, Diff: %d",
        round_points.points, round_points.hsd, round_points.prf, round_points.rb);
    sendcmd(&round_points);
}

void handle_free_message(void) {
    struct req_freemsg * freemsg_in = (struct req_freemsg *)local_data->recvbuf;
    logmsg("Request free message: Language %s", freemsg_in->language ? "English" : "Japanese");
    
    char query[256];
    snprintf(query, sizeof(query), "UPDATE accounts SET language = %d WHERE xuid = CONV('%016lX', 16, 10);", freemsg_in->language, local_data->xuid);
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to update language from free message, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    local_data->language = freemsg_in->language;
    
    // Show banned message
    if (local_data->banned) {
        const char * banmsg = local_data->language ? "You are banned from Campaign mode" : "";
        const size_t banmsg_len = strlen(banmsg) * sizeof(uint16_t);
    
        struct resp_freemsg * freemsg = malloc(sizeof(struct resp_freemsg) + banmsg_len);
        freemsg->code = 0x201A;
        freemsg->status = 0;
        freemsg->padding = 0;
        freemsg->msg_len = banmsg_len;
        
        for (int i = 0; i < strlen(banmsg); i++) {
            freemsg->msg[i] = banmsg[i];
        }
        
        freemsg->len = COMMAND_LEN(resp_freemsg) + freemsg->msg_len;
        logmsg("User is banned, show rejection message");
        sendcmd(freemsg);
        free(freemsg);
        return;
    }
    
    snprintf(query, sizeof(query), "SELECT freemsg_%s FROM settings ORDER BY round DESC, turn DESC LIMIT 1;", local_data->language ? "en" : "jp");
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query free message, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get free message, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 1) {
        logmsg("Incorrect number of fields (%d) in free message", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row) {
        logmsg("Failed to get row from free message, got error: %s", mysql_error(local_data->sqlcon));
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    char * rawmsg = row[0] ? row[0] : "";
    
    size_t msg_len = 0;
    for (int i = 0; rawmsg[i]; msg_len += sizeof(uint16_t)) {
        if      ((rawmsg[i] & 0x80) == 0x00) i++;
        else if ((rawmsg[i] & 0xE0) == 0xC0) i += 2;
        else if ((rawmsg[i] & 0xF0) == 0xE0) i += 3;
        else {
            // Don't support U+10000 codepoints
            logmsg("Unsupported UTF-8 sequence in free message");
            mysql_free_result(sqlres);
            client_handler_exit(1);
        }
    }
    
    logmsg("MOTD: \"%s\", length %ld", rawmsg, msg_len);
    
    struct resp_freemsg * freemsg = malloc(sizeof(struct resp_freemsg) + msg_len);
    freemsg->code = 0x201A;
    freemsg->status = 0;
    freemsg->padding = 0;
    freemsg->msg_len = msg_len;
    
    // Convert from UTF-8 to UTF-16 (this is not robust and probably wrong)
    for (int i = 0, j = 0; rawmsg[i]; j++) {
        if ((rawmsg[i] & 0x80) == 0x00) {
            freemsg->msg[j] = (uint16_t)rawmsg[i] & 0x7F;
            i++;
        } else if ((rawmsg[i] & 0xE0) == 0xC0) {
            freemsg->msg[j] = (((uint16_t)rawmsg[i] & 0x1F) << 6) | ((uint16_t)rawmsg[i + 1] & 0x3F);
            i += 2;
        } else if ((rawmsg[i] & 0xF0) == 0xE0) {
            freemsg->msg[j] = (((uint16_t)rawmsg[i] & 0x0F) << 12) | (((uint16_t)rawmsg[i + 1] & 0x3F) << 6) | ((uint16_t)rawmsg[i + 2] & 0x3F);
            i += 3;
        }
    }

    mysql_free_result(sqlres);
    
    freemsg->len = COMMAND_LEN(resp_freemsg) + freemsg->msg_len;
    sendcmd(freemsg);
    free(freemsg); // Cleanup
}

void handle_fixed_message(void) {
    logmsg("Request fixed message");
    
    struct resp_fixedmsg fixedmsg = {COMMAND_LEN(resp_fixedmsg), 0x201B};
    
    // No fixed messages for banned users
    if (local_data->banned) {
        logmsg("Disconnect banned user");
        sendcmd(&fixedmsg);
        return;
    }
    
    if (mysql_query(local_data->sqlcon, "SELECT round, turn, (UNIX_TIMESTAMP(period) - UNIX_TIMESTAMP(NOW())) FROM settings ORDER BY round DESC, turn DESC LIMIT 1;")) {
        logmsg("Failed to query data for fixed message, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get data for fixed message, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 3) {
        logmsg("Incorrect number of fields (%d) in data for fixed message", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row || !row[0] || !row[1] || !row[2]) {
        logmsg("Failed to get row from data for fixed message, got error: %s", mysql_error(local_data->sqlcon));
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    int round   = strtoul(row[0], NULL, 10);
    int turn    = strtoul(row[1], NULL, 10);
    long period = strtoul(row[2], NULL, 10);
    
    mysql_free_result(sqlres);
    
    if (!local_data->faction) {
        if (turn == 1) {
            // A new round has begun, pick a faction
            fixedmsg.msg[fixedmsg.msg_len++] = 0;
        } else {
            // You can now transfer to another faction
            fixedmsg.msg[fixedmsg.msg_len++] = 8;
        }
    }
    
    if (turn > 1 && period > 5 * DAY_SECONDS) {
        // A new turn has begun
        fixedmsg.msg[fixedmsg.msg_len++] = 1;
    }
    
    if (period < 2 * DAY_SECONDS) {
        if (turn == 8) {
            if (period < DAY_SECONDS) {
                // Round is over
                fixedmsg.msg[fixedmsg.msg_len++] = 4;
            }
            // Time remaining in round
            fixedmsg.msg[fixedmsg.msg_len++] = 3;
        } else {
            // Time remaining in turn
            fixedmsg.msg[fixedmsg.msg_len++] = 2;
        }
    }
    
    if (turn == 3) {
        // RB appears
        fixedmsg.msg[fixedmsg.msg_len++] = 7;
    } else if (turn == 5) {
        // Jar appears
        fixedmsg.msg[fixedmsg.msg_len++] = 10;
    }

    fixedmsg.len = COMMAND_LEN(resp_fixedmsg) + fixedmsg.msg_len;
    sendcmd(&fixedmsg);
}

void handle_get_user_info(void) {
    logmsg("Get user info");
    
    // Initialize struct as if user is unregistered
    struct resp_get_user_info get_user_info = {
        .len = COMMAND_LEN(resp_get_user_info), 
        .code = 0x2021,
        .status = 2,
        .data_size = PILOT_DATA_SIZE,
    };
    
    if (local_data->banned) {
        // Respond with an error to kick this user
        get_user_info.status = 1;
        sendcmd(&get_user_info);
        return;
    }
    
    char query[256];
    snprintf(query, sizeof(query),
        "SELECT pilot FROM accounts WHERE xuid = CONV('%016lX', 16, 10);",
        local_data->xuid);
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query user data, got error: %s", mysql_error(local_data->sqlcon));
        get_user_info.status = 1;
        sendcmd(&get_user_info);
        return;
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get user data, got error: %s", mysql_error(local_data->sqlcon));
        get_user_info.status = 1;
        sendcmd(&get_user_info);
        return;
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 1) {
        logmsg("Incorrect number of fields (%d) in user data", num_fields);
        mysql_free_result(sqlres);
        get_user_info.status = 1;
        sendcmd(&get_user_info);
        return;
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row || !row[0]) {
        logmsg("User is unregistered");
        mysql_free_result(sqlres);
        sendcmd(&get_user_info);
        return;
    }

    get_user_info.status = 0;
    get_user_info.faction = local_data->faction;
    memcpy(get_user_info.data, row[0], get_user_info.data_size);
    memcpy(get_user_info.name, row[0] + PILOT_NAME_OFFSET, PILOT_NAME_LEN);
    
    logmsg("Got pilot info: Faction \"%s\", Name \"%.16s\"", get_faction(local_data->faction), row[0] + PILOT_NAME_OFFSET);
    
    mysql_free_result(sqlres);
    sendcmd(&get_user_info);
}

void handle_new_user_info(void) {
    struct req_new_user_info * new_user_info_in = (struct req_new_user_info *)local_data->recvbuf;
    logmsg("New user info: Pilot Name \"%s\", XLive Name \"%s\", Faction \"%s\"",
        new_user_info_in->name, new_user_info_in->xname, get_faction(new_user_info_in->faction));
    
    if (new_user_info_in->data_size != PILOT_DATA_SIZE) {
        logmsg("Warning pilot data in length was %d not %d", new_user_info_in->data_size, PILOT_DATA_SIZE);
        client_handler_exit(1);
    }
    
    char pilot_escape[2 * PILOT_DATA_SIZE + 1];
    mysql_real_escape_string(local_data->sqlcon, pilot_escape, new_user_info_in->data, PILOT_DATA_SIZE);
    
    char query[2 * PILOT_DATA_SIZE + 256];
    size_t query_len = snprintf(query, sizeof(query),
        "UPDATE accounts SET xname = '%s', faction = %d, pilot = '%s' WHERE xuid = CONV('%016lX', 16, 10);",
        new_user_info_in->xname, new_user_info_in->faction, pilot_escape, local_data->xuid);
    // At this point query may contain null bytes other than the terminator
    struct resp_status new_user_info = {COMMAND_LEN(resp_status), 0x6022};
    
    if (mysql_real_query(local_data->sqlcon, query, query_len)) {
        logmsg("Failed to update new user data, got error: %s", mysql_error(local_data->sqlcon));
        new_user_info.status = 1;
    }

    local_data->faction = new_user_info_in->faction;
    
    sendcmd(&new_user_info);
}

void handle_set_user_info(void) {
    struct req_set_user_info * set_user_info_in = (struct req_set_user_info *)local_data->recvbuf;
    logmsg("Set user info: Pilot Name \"%.16s\"", set_user_info_in->data + PILOT_NAME_OFFSET);
    
    if (set_user_info_in->data_size != PILOT_DATA_SIZE) {
        logmsg("Warning pilot data in length was %d not %d", set_user_info_in->data_size, PILOT_DATA_SIZE);
        client_handler_exit(1);
    }
    
    char pilot_escape[2 * PILOT_DATA_SIZE + 1];
    mysql_real_escape_string(local_data->sqlcon, pilot_escape, set_user_info_in->data, PILOT_DATA_SIZE);
    
    size_t query_max_len = 2 * PILOT_DATA_SIZE + 256;
    char query[query_max_len];
    size_t query_len = snprintf(query, query_max_len,
        "UPDATE accounts SET pilot = '%s' WHERE xuid = CONV('%016lX', 16, 10);",
        pilot_escape, local_data->xuid);
    
    struct resp_status set_user_info = {COMMAND_LEN(resp_status), 0x6024};
    
    if (mysql_real_query(local_data->sqlcon, query, query_len)) {
        logmsg("Failed to update set user data, got error: %s", mysql_error(local_data->sqlcon));
        set_user_info.status = 1;
    }
    
    if (!mysql_affected_rows(local_data->sqlcon)) {
        logmsg("Did not update set user data, zero rows affected");
        set_user_info.status = 1;
    }
    
    sendcmd(&set_user_info);
}

void handle_del_user_info(void) {
    logmsg("Request delete user info");

    struct resp_status del_user_info = {COMMAND_LEN(resp_status), 0x2023, 1};

    char query[256];
    
    // Copy pilot to graveyard
    snprintf(query, sizeof(query), "INSERT INTO graveyard (xuid, pilot, tod) SELECT xuid, pilot, NOW() FROM accounts WHERE xuid = CONV('%016lX', 16, 10);",
        local_data->xuid);
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to add pilot to the graveyard durring delete user data, got error: %s", mysql_error(local_data->sqlcon));
    }
    
    // Delete Pilot Data
    snprintf(query, sizeof(query), "UPDATE accounts SET faction = 0, pilot = NULL WHERE xuid = CONV('%016lX', 16, 10);",
        local_data->xuid);
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to delete user data, got error: %s", mysql_error(local_data->sqlcon));
        sendcmd(&del_user_info);
        return;
    }
    
    if (!mysql_affected_rows(local_data->sqlcon)) {
        logmsg("Did not delete user data, zero rows affected");
        sendcmd(&del_user_info);
        return;
    }

    logmsg("Pilot data deleted");
    
    // Delete all VTs from garage
    snprintf(query, sizeof(query), "DELETE FROM garage WHERE xuid = CONV('%016lX', 16, 10);", local_data->xuid);
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to delete garage data, got error: %s", mysql_error(local_data->sqlcon));
        sendcmd(&del_user_info);
        return;
    }
    
    // Delete leaderboard entries
    snprintf(query, sizeof(query), "DELETE FROM leaderboard WHERE xuid = CONV('%016lX', 16, 10);", local_data->xuid);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to delete leaderboard data, got error: %s", mysql_error(local_data->sqlcon));
        sendcmd(&del_user_info);
        return;
    }

    logmsg("Garage data deleted");
    del_user_info.status = 0;
    sendcmd(&del_user_info);
}

void handle_set_user_faction(void) {
    struct req_set_user_faction * set_user_faction_in = (struct req_set_user_faction *)local_data->recvbuf;
    logmsg("Request join faction: Faction \"%s\"", get_faction(set_user_faction_in->faction));

    // Need to kick the user out of Campaign so that their garage reloads correctly
    struct resp_status set_user_faction = {COMMAND_LEN(resp_status), 0x6025, 1};
    
    char query[256];
    snprintf(query, sizeof(query),
        "UPDATE accounts SET faction = %d WHERE xuid = CONV('%016lX', 16, 10);",
        set_user_faction_in->faction, local_data->xuid);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to update faction affiliation, got error: %s", mysql_error(local_data->sqlcon));
        sendcmd(&set_user_faction);
        return;
    }
    
    if (!mysql_affected_rows(local_data->sqlcon)) {
        logmsg("Did not update faction affiliation, zero rows affected");
        sendcmd(&set_user_faction);
        return;
    }
    
    local_data->faction = set_user_faction_in->faction;

    logmsg("Faction affiliation updated");
    sendcmd(&set_user_faction);
}

void handle_vt_owned(void) {
    logmsg("Request VT owned: Faction \"%s\"", get_faction(local_data->faction));
    
    char query[256];
    snprintf(query, sizeof(query),
        "DELETE FROM garage WHERE sortied = 1 AND faction = %d AND xuid = CONV('%016lX', 16, 10);",
        local_data->faction, local_data->xuid);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to delete lost VTs from garage, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    snprintf(query, sizeof(query),
        "SELECT type, serial FROM garage WHERE sortied = 0 AND faction = %d AND xuid = CONV('%016lX', 16, 10);",
        local_data->faction, local_data->xuid);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query owned VTs, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get owned VTs, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 2) {
        logmsg("Incorrect number of fields (%d) in owned VTs", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    uint16_t vt_count = mysql_num_rows(sqlres);
    struct resp_vt_list * vt_owned = malloc(sizeof(struct resp_vt_list) + sizeof(struct vt_entry) * vt_count);
    vt_owned->code = 0x2040;
    vt_owned->status = 0;
    vt_owned->vts.len = vt_count;
    vt_owned->len = COMMAND_LEN(resp_vt_list) + sizeof(struct vt_entry) * vt_count;
    
    MYSQL_ROW row;
    for (int i = 0; i < vt_count; i++) {
        row = mysql_fetch_row(sqlres);
        vt_owned->vts.list[i].type   = strtoul(row[0], NULL, 0);
        vt_owned->vts.list[i].serial = strtoul(row[1], NULL, 0);
    }
    
    mysql_free_result(sqlres);
    
    logmsg("VT owned response: %d VTs", vt_count);
    sendcmd(vt_owned);
    free(vt_owned); // Cleanup
}

void handle_vt_add(void) {
    struct req_vt_add * vt_add_in = (struct req_vt_add *)local_data->recvbuf;
    logmsg("Request VT add: %d VTs, Faction: \"%s\"", vt_add_in->list_len, get_faction(local_data->faction));
    
    #define ADD_LIST_LEN 32
    uint16_t add_list[ADD_LIST_LEN] = {0};
    
    int add_list_total = 0;
    for (int i = 0; i < vt_add_in->list_len; i++) {
        uint16_t type = vt_add_in->list[i];
        if (type < ADD_LIST_LEN) {
            add_list[type]++;
            add_list_total++;
        }
        else logmsg("Cannot add VT, ID %d out of range", type);
    }
    
    if (mysql_set_server_option(local_data->sqlcon, MYSQL_OPTION_MULTI_STATEMENTS_ON)) {
        logmsg("Failed to enable multi-statement during VT add, got error: %s",  mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    struct resp_vt_list * vt_add = malloc(sizeof(struct resp_vt_list) + sizeof(struct vt_entry) * add_list_total);
    vt_add->code = 0x2042;
    vt_add->status = 0;
    vt_add->vts.len = add_list_total;
    vt_add->len = COMMAND_LEN(resp_vt_list) + sizeof(struct vt_entry) * add_list_total;
    
    for (int type = 0, vt_add_pos = 0; type < ADD_LIST_LEN; type++) {
        if (!add_list[type]) continue;
        int quantity = add_list[type];
        
        char query[512];
        snprintf(query, sizeof(query),
            "BEGIN; SELECT serial FROM shop WHERE type = %d FOR UPDATE; UPDATE shop SET serial = serial + %d, vtstock = GREATEST(vtstock - %d, 0) WHERE type = %d; COMMIT;",
            type, quantity, quantity, type);
    
        if (mysql_query(local_data->sqlcon, query)) {
            logmsg("Failed to update serials in add list, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
        
        // Skip the BEGIN statement
        if (mysql_next_result(local_data->sqlcon) > 0) {
            logmsg("Error while skipping first result from add list");
            client_handler_exit(1);
        }
        
        MYSQL_RES * sqlres;
        if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
            logmsg("Failed to get serial number for add list, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
        
        int num_fields = mysql_num_fields(sqlres);
        if (num_fields != 1) {
            logmsg("Incorrect number of fields (%d) in add list", num_fields);
            mysql_free_result(sqlres);
            client_handler_exit(1);
        }
        
        MYSQL_ROW row = mysql_fetch_row(sqlres);
        if (!row || !row[0]) {
            logmsg("Failed to get row from add list");
            mysql_free_result(sqlres);
            client_handler_exit(1);
        }

        uint32_t serial_base = strtoul(row[0], NULL, 10);
        mysql_free_result(sqlres);
        
        int status;
        do {
            status = mysql_next_result(local_data->sqlcon);
            if (status > 0) {
                logmsg("Error while discarding extra results from add list");
                client_handler_exit(1);
            }
        } while (!status);
        
        for (int i = 0; i < quantity; i++) {
            snprintf(query, sizeof(query),
                "INSERT INTO garage SET type = %d, serial = %d, faction = %d, xuid = CONV('%016lX', 16, 10);",
                type, serial_base + i, local_data->faction, local_data->xuid);
            
            if (mysql_query(local_data->sqlcon, query)) {
                logmsg("Failed to add VTs in add list, got error: %s", mysql_error(local_data->sqlcon));
                client_handler_exit(1);
            }
            
            vt_add->vts.list[vt_add_pos].type = type;
            vt_add->vts.list[vt_add_pos].serial = serial_base + i;
            vt_add_pos++;
        }
    }
    
    logmsg("Add VT response: %d VTs added", add_list_total);
    
    sendcmd(vt_add);
    free(vt_add);
}

void handle_vt_del(void) {
    struct req_vt_list * vt_del_in = (struct req_vt_list *)local_data->recvbuf;
    logmsg("Request VT delete: Deleting %d VTs", vt_del_in->vts.len);
    
    struct resp_vt_list vt_del = {COMMAND_LEN(resp_vt_list), 0x2044};

    int affected_count = 0;    
    for (int i = 0; i < vt_del_in->vts.len; i++) {
        char query[256];
        snprintf(query, sizeof(query),
            "DELETE FROM garage WHERE type = %d AND serial = %d AND faction = %d AND xuid = CONV('%016lX', 16, 10);",
            vt_del_in->vts.list[i].type, vt_del_in->vts.list[i].serial, local_data->faction, local_data->xuid);
            
        if (mysql_query(local_data->sqlcon, query)) {
            logmsg("Failed to delete VT from garage, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
        
        affected_count += mysql_affected_rows(local_data->sqlcon);
    }
    
    logmsg("Delete VT response: Deleted %d VTs\n", affected_count);
    sendcmd(&vt_del);
}

void handle_vt_sortie(void) {
    struct req_vt_list * vt_sortie_in = (struct req_vt_list *)local_data->recvbuf;
    logmsg("Request VT sortie: %d VTs sortied", vt_sortie_in->vts.len);
    
    struct resp_status vt_sortie = {COMMAND_LEN(resp_status), 0x2047};
    
    int affected_count = 0;
    for (int i = 0; i < vt_sortie_in->vts.len; i++) {
        char query[256];
        snprintf(query, sizeof(query),
            "UPDATE garage SET sortied = 1 WHERE type = %d AND serial = %d AND faction = %d AND xuid = CONV('%016lX', 16, 10);",
            vt_sortie_in->vts.list[i].type, vt_sortie_in->vts.list[i].serial, local_data->faction, local_data->xuid);
            
        if (mysql_query(local_data->sqlcon, query)) {
            logmsg("Failed to sortie VT from garage, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
        
        affected_count += mysql_affected_rows(local_data->sqlcon);
    }
    
    logmsg("Sortie VT response: Sortied %d VTs", affected_count);
    sendcmd(&vt_sortie);
}

void handle_vt_return(void) {
    struct req_vt_list * vt_return_in = (struct req_vt_list *)local_data->recvbuf;
    logmsg("Request VT return: %d VTs returned", vt_return_in->vts.len);
    
    struct resp_vt_list * vt_return = malloc(sizeof(struct resp_vt_list) + sizeof(struct vt_entry) * vt_return_in->vts.len);
    vt_return->code = 0x4047;
    vt_return->status = 0;
    
    char query[256];
    
    vt_return->vts.len = 0;
    for (int i = 0; i < vt_return_in->vts.len; i++) {
        uint16_t type = vt_return_in->vts.list[i].type;
        uint32_t serial = vt_return_in->vts.list[i].serial;
    
        snprintf(query, sizeof(query),
            "UPDATE garage SET sortied = 0 WHERE type = %d AND serial = %d AND faction = %d AND xuid = CONV('%016lX', 16, 10);",
            type, serial, local_data->faction, local_data->xuid);
            
        if (mysql_query(local_data->sqlcon, query)) {
            logmsg("Failed to return VT to garage, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
            
        if (mysql_affected_rows(local_data->sqlcon)) {
            uint16_t pos = vt_return->vts.len;
            vt_return->vts.list[pos].type = type;
            vt_return->vts.list[pos].serial = serial;
            vt_return->vts.len++;
        } else {
            logmsg("Failed to return VT to garage, no affected rows: Type %d, Serial %010d", type, serial);
        }
    }
    vt_return->len = COMMAND_LEN(resp_vt_list) + sizeof(struct vt_entry) * vt_return->vts.len;
    
    if (vt_return->vts.len != vt_return_in->vts.len) vt_return->status = 1;

    snprintf(query, sizeof(query),
        "DELETE FROM garage WHERE sortied = 1 AND faction = %d AND xuid = CONV('%016lX', 16, 10);",
        local_data->faction, local_data->xuid);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to delete lost VTs from garage, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }

    int lost_vt_count = mysql_affected_rows(local_data->sqlcon);

    logmsg("VT return response: %d VTs returned, %d VTs lost", vt_return->vts.len, lost_vt_count);
    sendcmd(vt_return);
    free(vt_return);
}

void handle_vt_capture(void) {
    struct req_vt_capture * vt_capture_in = (struct req_vt_capture *)local_data->recvbuf;
    logmsg("Request VT capture: %d VTs captured, Name %.16s", vt_capture_in->vts.len, vt_capture_in->name);
    
    struct resp_vt_capture * vt_capture = malloc(sizeof(struct resp_vt_capture) + sizeof(struct vt_entry) * vt_capture_in->vts.len);
    vt_capture->code = 0x2046;
    vt_capture->status = 0;
    memcpy(vt_capture->name, vt_capture_in->name, PILOT_NAME_LEN);
    
    for (int i = 0; i < vt_capture_in->vts.len; i++) {
        uint16_t type = vt_capture_in->vts.list[i].type;
        uint32_t serial = vt_capture_in->vts.list[i].serial;
        
        if (!serial) {
            logmsg("Skipping VT type %d with null serial", type);
            continue;
        }
        
        char query[256];
        snprintf(query, sizeof(query),
            "INSERT INTO garage (type, serial, xuid, faction) VALUES (%d, %d, CONV('%016lX', 16, 10), %d) ON DUPLICATE KEY UPDATE sortied = 0, faction = %d, xuid = CONV('%016lX', 16, 10);",
            type, serial, local_data->xuid, local_data->faction,
            local_data->faction, local_data->xuid);
        
        if (mysql_query(local_data->sqlcon, query)) {
            logmsg("Failed to add captured VT to garage, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
        
        if (mysql_affected_rows(local_data->sqlcon)) {
            uint16_t pos = vt_capture->vts.len;
            vt_capture->vts.list[pos].type = type;
            vt_capture->vts.list[pos].serial = serial;
            vt_capture->vts.len++;
        } else {
            logmsg("Failed to add captured VT to garage, no affected rows: Type %d, Serial %010d", type, serial);
        }
    }
    
    vt_capture->len = COMMAND_LEN(resp_vt_capture) + sizeof(struct vt_entry) * vt_capture->vts.len;
    
    logmsg("VT capture response: %d VTs captured, Name %.16s", vt_capture->vts.len, vt_capture->name);
    sendcmd(vt_capture);
    free(vt_capture);
}

void handle_shop_list(void) {
    logmsg("Request shop list: Faction \"%s\"", get_faction(local_data->faction));
    
    char query[512];
    snprintf(query, sizeof(query),
        "SELECT type, cost, vtstock FROM shop WHERE (faction = %d %s) AND IFNULL(vtstock, 1) > 0 AND turn <= (SELECT turn FROM settings ORDER BY round DESC, turn DESC LIMIT 1);",
        local_data->faction,
        local_data->faction == 4 ? "OR type = 0 OR type = 1" : "" // Give Jar access to Vitzh and m-Vitzh
    );
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query shop list, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get shop list, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 3) {
        logmsg("Incorrect number of fields (%d) in shop list", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    size_t rowcount = mysql_num_rows(sqlres);
    struct resp_shop_list * shop_list = malloc(sizeof(struct resp_shop_list) + sizeof(struct shop_entry) * rowcount);
    shop_list->code = 0x2041;
    shop_list->status = 0;
    shop_list->list_len = rowcount;
    shop_list->len = COMMAND_LEN(resp_shop_list) + sizeof(struct shop_entry) * rowcount;
    
    MYSQL_ROW row;
    for (int i = 0; i < rowcount; i++) {
        row = mysql_fetch_row(sqlres);
        shop_list->list[i].type  = strtoul(row[0], NULL, 10);
        shop_list->list[i].cost  = strtoul(row[1], NULL, 10);
        shop_list->list[i].limit = row[2] ? strtoul(row[2], NULL, 10) : -1;
        
        shop_list->list[i].padding = 0;
    }
    
    mysql_free_result(sqlres);
    
    logmsg("Shop list response, %d entries", rowcount);
    
    sendcmd(shop_list);
    free(shop_list);
}

void handle_analyze(void) {
    struct req_analyze * analyze_in = (struct req_analyze *)local_data->recvbuf;
    logmsg("Request analyze");

    // Figure out what the hell this data is
    for (int i = 0; i < 10; i++) {
        logmsg("Analyze data: %d", analyze_in->data[i]);
    }
    
    struct command_base analyze = {0, 0x2090};
    sendcmd(&analyze);
}

void handle_add_user_point(void) {
    struct req_add_user_point * add_user_point_in = (struct req_add_user_point *)local_data->recvbuf;
    logmsg("Request add user point: Faction \"%s\", Round %d, Turn %d, Map %d", 
        get_faction(local_data->faction), add_user_point_in->round,
        add_user_point_in->turn, add_user_point_in->map);
        
        /*
            stat1 = Shots hit
            stat2 = Shots fired
            stat3 = VTs killed
            stat4 = ??? Points for killed VTs ???
            stat5 = VTs lost
            stat6 = Victories
            stat7 = Matches played
            stat8 = Occupations (Base Captures)
            stat9 = ??? VT Stolen ???
            stat10 = Tipping
        */
/*        
    if (add_user_point_in->points[0] == add_user_point_in->points[1]) {
        // Prevent 100% accuracy
        add_user_point_in->points[1]++;
    }
*/
    char query[1024];
    snprintf(query, sizeof(query),
        "INSERT INTO leaderboard (xuid, faction, round, turn, map, stat1, stat2, stat3, stat4, stat5, stat6, stat7, stat8, stat9, stat10) VALUES (CONV('%016lX', 16, 10), %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d) ON DUPLICATE KEY UPDATE stat1 = stat1 + %d, stat2 = stat2 + %d, stat3 = stat3 + %d, stat4 = stat4 + %d, stat5 = stat5 + %d, stat6 = stat6 + %d, stat7 = stat7 + %d, stat8 = stat8 + %d, stat9 = stat9 + %d, stat10 = stat10 + %d;",
        // Primary keys
        local_data->xuid, local_data->faction, add_user_point_in->round, add_user_point_in->turn, add_user_point_in->map,
        // Points
        add_user_point_in->points[0], add_user_point_in->points[1], add_user_point_in->points[2], add_user_point_in->points[3], add_user_point_in->points[4], add_user_point_in->points[5], add_user_point_in->points[6], add_user_point_in->points[7], add_user_point_in->points[8], add_user_point_in->points[9],
        // Points (again)
        add_user_point_in->points[0], add_user_point_in->points[1], add_user_point_in->points[2], add_user_point_in->points[3], add_user_point_in->points[4], add_user_point_in->points[5], add_user_point_in->points[6], add_user_point_in->points[7], add_user_point_in->points[8], add_user_point_in->points[9]);

    struct resp_status add_user_point = {COMMAND_LEN(resp_status), 0x6026};
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to add user points, got error: %s", mysql_error(local_data->sqlcon));
        add_user_point.status = 1;
    }
    
    sendcmd(&add_user_point);
}

void handle_add_mission_point(void) {
    struct req_add_mission_point * add_mission_point_in = (struct req_add_mission_point *)local_data->recvbuf;
    logmsg("Request add mission point: Round %d, Turn %d, Map %d, Points %d, Faction \"%s\"", add_mission_point_in->round,
        add_mission_point_in->turn, add_mission_point_in->map,
        add_mission_point_in->points, get_faction(add_mission_point_in->faction));

    int map_id = add_mission_point_in->map + (add_mission_point_in->turn - 1) * 3;

    struct resp_status add_mission_point = {COMMAND_LEN(resp_status), 0x6027};
        
    char * faction = NULL;
    switch (add_mission_point_in->faction) {
    case 1: faction = "hsd"; break;
    case 2: faction = "prf"; break;
    case 3: faction = "rb";  break;
    default:
        logmsg("Can't add points for faction (%d) \"%s\"", add_mission_point_in->faction, get_faction(add_mission_point_in->faction));
        add_mission_point.status = 1;
        sendcmd(&add_mission_point);
        return;
    }

    char query[256];
    snprintf(query, sizeof(query),
        "INSERT INTO wartide (round, map, %s) VALUES(%d, %d, %d) ON DUPLICATE KEY UPDATE %s = %s + %d;",
        faction,
        add_mission_point_in->round, map_id, add_mission_point_in->points,
        faction, faction, add_mission_point_in->points
    );
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to update wartide, got error: %s", mysql_error(local_data->sqlcon));
        add_mission_point.status = 1;
    }
    
    sendcmd(&add_mission_point);
}

void handle_message_count(void) {
    logmsg("Request message count (disabled)");
    
    // Send back a status=1 response to disable this feature
    struct resp_message_count message_count = {COMMAND_LEN(resp_message_count), 0x2050, 1};
    sendcmd(&message_count);
}

void handle_message_post(void) {
    logmsg("Request message post (disabled)");
    
    // Send back a status=1 response to disable this feature
    struct resp_message_post message_post = {COMMAND_LEN(resp_message_post), 0x2052, 1};
    sendcmd(&message_post);
}

void handle_rank_count(uint32_t * count, uint32_t * place, uint16_t round, uint16_t turn, uint8_t category, uint8_t faction) {
    char turn_query[32] = "";
    if (turn) snprintf(turn_query, sizeof(turn_query), "AND turn = %d", turn);


    char faction_query[64] = "";
    if (faction) snprintf(faction_query, sizeof(faction_query), "AND faction = %d", faction);

    char query[512];
    snprintf(query, sizeof(query),
        "SELECT COUNT(*) FROM (SELECT 0 FROM leaderboard WHERE round = %d %s %s GROUP BY xuid) lb;",
        round, turn_query, faction_query);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query rank index, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get rank index, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 1) {
        logmsg("Incorrect number of fields (%d) in rank index", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row || !row[0]) {
        logmsg("Failed to get row from rank index");
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }

    *count = strtoul(row[0], NULL, 10);
    mysql_free_result(sqlres);

    // Don't compute the place
    if (!place) return;
    
    char * category_query = NULL;
    switch (category) {
    case 1: category_query = "FLOOR(SUM(stat1) / (SUM(stat2) + 1) * 1000)"; break; // Accuracy
    case 3: category_query = "SUM(stat4)";                                  break; // Points for Enemies Destroyed
    case 5: category_query = "FLOOR(SUM(stat6) / SUM(stat7) * 1000)";       break; // Mission Victories
    case 6: category_query = "SUM(stat8)";                                  break; // Number of Occupations
    
    case 2: category_query = "SUM(stat3)";                                  break; // Number of Enemies Destroyed
    case 4: category_query = "SUM(stat5)";                                  break; // Number of VTs Lost
    case 7: category_query = "SUM(stat9)";                                  break; // Number of VTs Captured
    case 8: category_query = "SUM(stat10)";                                 break; // Number of Tippings
    default:
        logmsg("Unknown category %d", category);
        client_handler_exit(1);
    }
    
    snprintf(query, sizeof(query),
        "SELECT `rank` FROM (SELECT xuid, ROW_NUMBER() OVER(ORDER BY score DESC) `rank` FROM (SELECT xuid, %s AS score FROM leaderboard WHERE round = %d %s %s GROUP BY xuid) as lb) as lb_rank WHERE xuid = CONV('%016lX', 16, 10);",
        category_query,
        round,
        turn_query,
        faction_query,
        local_data->xuid
    );
    
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query rank place, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get rank place, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    num_fields = mysql_num_fields(sqlres);
    if (num_fields != 1) {
        logmsg("Incorrect number of fields (%d) in rank place", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    row = mysql_fetch_row(sqlres);
    if (!row || !row[0]) {
        *place = 0; // User wasn't ranked
        mysql_free_result(sqlres);
        return;
    }
    
    *place = strtoul(row[0], NULL, 10);
    mysql_free_result(sqlres);
}

void handle_rank_count_global(void) {
    struct req_rank_count_global * rank_count_global_in = (struct req_rank_count_global *)local_data->recvbuf;
    logmsg("Request rank index global: Round %d, Turn, %d, Category %d",
        rank_count_global_in->round, rank_count_global_in->turn, rank_count_global_in->category);
        
    struct resp_rank_count rank_count = {COMMAND_LEN(resp_rank_count), 0x2061};
    uint32_t count, place;
    handle_rank_count(&count, &place, rank_count_global_in->round, rank_count_global_in->turn, rank_count_global_in->category, 0);
    
    if (config.game == LineOfContact) {
        rank_count.loc.count = count;
        rank_count.loc.place = place;
    } else {
        rank_count.pt.count = count;
    }
    
    logmsg("Rank index global response: Count %d, Place %d", count, place);
    sendcmd(&rank_count);
}

void handle_rank_count_faction(void) {
    struct req_rank_count_faction * rank_count_faction_in = (struct req_rank_count_faction *)local_data->recvbuf;

    if (config.game != LineOfContact) {
        // For some reason the PilotTest returns "faction - 1", so correct that here
        rank_count_faction_in->faction++;
    }

    logmsg("Request rank index faction: Round %d, Turn, %d, Category %d, Faction \"%s\"",
        rank_count_faction_in->round, rank_count_faction_in->turn,
        rank_count_faction_in->category, get_faction(rank_count_faction_in->faction));

    struct resp_rank_count rank_count = {COMMAND_LEN(resp_rank_count), 0x2062};
    uint32_t count, place;
    handle_rank_count(&count, &place, rank_count_faction_in->round, rank_count_faction_in->turn,
        rank_count_faction_in->category, rank_count_faction_in->faction);

    if (config.game == LineOfContact) {
        rank_count.loc.count = count;
        rank_count.loc.place = place;
    } else {
        rank_count.pt.count = count;
    }
    
    logmsg("Rank index faction response: Count %d, Place %d", count, place);
    sendcmd(&rank_count);
}

void handle_rank_count_categories(void) {
    struct req_round_turn * rank_count_categories_in = (struct req_round_turn *)local_data->recvbuf;
    logmsg("Request rank count categories: Round %d, Turn %d", rank_count_categories_in->round, rank_count_categories_in->turn);
    
    struct resp_rank_count_categories rank_count_categories = {COMMAND_LEN(resp_rank_count_categories), 0x2063};

    uint32_t count;
    handle_rank_count(&count, NULL, rank_count_categories_in->round, rank_count_categories_in->turn, 1, 0);    

    // This will be the same response for all 8 categories
    for (int i = 0; i < 8; i++) {
        rank_count_categories.count[i] = count;
    }

    logmsg("Rank count categories response: %d %d %d %d %d %d %d %d",
        rank_count_categories.count[0], rank_count_categories.count[1],
        rank_count_categories.count[2], rank_count_categories.count[3],
        rank_count_categories.count[4], rank_count_categories.count[5],
        rank_count_categories.count[6], rank_count_categories.count[7]);
    sendcmd(&rank_count_categories);
}

void handle_rank_values(void) {
    struct req_rank_values * rank_values_in = (struct req_rank_values *)local_data->recvbuf;
    logmsg("Request rank values: Round %d, Turn %d, Faction \"%s\", Category %d, Start %d, Count %d",
        rank_values_in->round, rank_values_in->turn, get_faction(rank_values_in->faction),
        rank_values_in->category, rank_values_in->start, rank_values_in->count);
    
    char turn_query[32];
    snprintf(turn_query, sizeof(turn_query), "AND leaderboard.turn = %d", rank_values_in->turn);
    
    char faction_query[64];
    snprintf(faction_query, sizeof(faction_query), "AND leaderboard.faction = %d", rank_values_in->faction);
    
    char * category_query = NULL;
    switch (rank_values_in->category) {
    case 1: category_query = "FLOOR(SUM(stat1) / (SUM(stat2) + 1) * 1000)"; break; // Accuracy
    case 3: category_query = "SUM(stat4)";                                  break; // Points for Enemies Destroyed
    case 5: category_query = "FLOOR(SUM(stat6) / SUM(stat7) * 1000)";       break; // Mission Victories
    case 6: category_query = "SUM(stat8)";                                  break; // Number of Occupations
    
    case 2: category_query = "SUM(stat3)";                                  break; // Number of Enemies Destroyed
    case 4: category_query = "SUM(stat5)";                                  break; // Number of VTs Lost
    case 7: category_query = "SUM(stat9)";                                  break; // Number of VTs Captured
    case 8: category_query = "SUM(stat10)";                                 break; // Number of Tippings
    default:
        logmsg("Unknown category %d", rank_values_in->category);
        client_handler_exit(1);
    }
    
    char query[512];
    snprintf(query, sizeof(query),
        "SELECT accounts.xname, accounts.pilot, (%s) AS res FROM leaderboard INNER JOIN accounts ON leaderboard.xuid = accounts.xuid WHERE leaderboard.round = %d %s %s GROUP BY leaderboard.xuid ORDER BY res DESC LIMIT %d, %d;",
        category_query,
        rank_values_in->round,
        rank_values_in->turn ? turn_query : "",
        rank_values_in->faction ? faction_query : "",
        rank_values_in->start - 1, rank_values_in->count);

    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query rank name, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get rank name, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 3) {
        logmsg("Incorrect number of fields (%d) in rank name", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }

    size_t rank_count = mysql_num_rows(sqlres);
    struct resp_rank_values * rank_values = malloc(sizeof(struct resp_rank_values) + sizeof(struct rank_entry) * rank_count);
    rank_values->code = 0x2060;
    rank_values->status = 0;
    rank_values->a = rank_count;
    
    rank_values->ranks.len = rank_count;
    rank_values->len = COMMAND_LEN(resp_rank_values) + sizeof(struct rank_entry) * rank_count;
    
    for (int i = 0; i < rank_count; i++) {
        MYSQL_ROW row = mysql_fetch_row(sqlres);
        if (!row) {
            logmsg("Failed to get row %d", i);
            mysql_free_result(sqlres);
            client_handler_exit(1);
        }
    
        strncpy(rank_values->ranks.list[i].xname, row[0], PILOT_NAME_LEN);
        
        if (row[1]) memcpy(rank_values->ranks.list[i].name, row[1] + PILOT_NAME_OFFSET, PILOT_NAME_LEN);
        else bzero(rank_values->ranks.list[i].name, PILOT_NAME_LEN);
        
        rank_values->ranks.list[i].rank = rank_values_in->start + i;
        rank_values->ranks.list[i].a = 0;
        rank_values->ranks.list[i].score = row[2] ? strtoul(row[2], NULL, 10) : 0;
    }

    mysql_free_result(sqlres);
    
    logmsg("Rank values response: %d Ranks", rank_count);
    sendcmd(rank_values);
    free(rank_values);
}

void handle_awards_available(void) {
    logmsg("Request awards available");
    
    // TODO
    
    size_t award_count = 0;
    struct resp_award_available * award_available = malloc(sizeof(struct resp_award_available) + sizeof(struct award_entry) * award_count);
    award_available->code = 0x6041;
    award_available->status = 0;
    award_available->awards.len = award_count;
    award_available->len = COMMAND_LEN(resp_award_available) + sizeof(struct award_entry) * award_count;
    
    logmsg("Awards available response: %d Awards", award_count);
    sendcmd(award_available);
    free(award_available);
}

void handle_trade_token(void) {
    struct req_trade_token * trade_token_in = (struct req_trade_token *)local_data->recvbuf;
    logmsg("Request trade token: Partner Pilot Name \"%.16s\"", trade_token_in->name);
    
    struct resp_trade_token trade_token = {COMMAND_LEN(resp_trade_token), 0x204A};
    
    // Increment trade token
    pthread_mutex_lock(&trade_mtx);
    trade_token.token = current_token++;
    pthread_mutex_unlock(&trade_mtx);
    
    logmsg("Trade token response: Token %d", trade_token.token);
    sendcmd(&trade_token);
}

bool wait_for_partner(uint16_t code, uint32_t token) {
    bool complete = false;
    int trade_slot = -1;

    // Find/create a trade slot and mark it as used    
    pthread_mutex_lock(&trade_mtx);
    for (int i = 0; i < TRADE_COUNT; i++) {
        if (trades[i].token == token && trades[i].code == code && trades[i].state == trade_state_waiting) {
            logmsg("Trade partner already waiting, signaling ready to them: Code %04X", code);

            trades[i].state = trade_state_complete;
            trade_slot = i;
            complete = true;

            break;
        } else if (trade_slot < 0 && trades[i].state == trade_state_idle) {
            trade_slot = i;
        }
    }
    
    if (trade_slot < 0) {
        logmsg("No free trade slots");
    } else if (!complete) {
        logmsg("Waiting for trade partner: Code %04X", code);
        
        trades[trade_slot].token = token;
        trades[trade_slot].code = code;
        trades[trade_slot].state = trade_state_waiting;
    }
    pthread_mutex_unlock(&trade_mtx);
    
    // Wait for trade partner to make their request
    if (!complete && trade_slot >= 0) {
        const int wait_seconds = 5;
        for (int i = 0; i <= wait_seconds && !complete; i++) {
            sleep(1);
            
            pthread_mutex_lock(&trade_mtx);
            complete = trades[trade_slot].state == trade_state_complete;
            // Cleanup on the last iteration of the wait loop
            if (i == wait_seconds || complete) trades[trade_slot].state = trade_state_idle;
            pthread_mutex_unlock(&trade_mtx);
        }
        
        if (complete) {
            logmsg("Got partner ready signal");
        } else {
            logmsg("Gave up on waiting for partner");
        }
    }
    
    return complete;
}

void handle_trade_user_info(void) {
    struct req_trade_user_info * trade_user_info_in = (struct req_trade_user_info *)local_data->recvbuf;
    logmsg("Request trade user info: Partner Pilot Name \"%.16s\", Token %d", trade_user_info_in->name, trade_user_info_in->token);
    
    if (trade_user_info_in->data_size != PILOT_DATA_SIZE) {
        logmsg("Warning pilot data in length was %d not %d", trade_user_info_in->data_size, PILOT_DATA_SIZE);
    }
    
    struct resp_trade_user_info trade_user_info = {COMMAND_LEN(resp_trade_user_info), 0x204B};
    
    bool success = wait_for_partner(trade_user_info_in->code, trade_user_info_in->token);
    
    if (!success) {
        trade_user_info.status = 1;
        sendcmd(&trade_user_info);
        return;
    }
    
    char pilot_escape[2 * PILOT_DATA_SIZE + 1];
    mysql_real_escape_string(local_data->sqlcon, pilot_escape, trade_user_info_in->data, PILOT_DATA_SIZE);
    
    size_t query_max_len = 2 * PILOT_DATA_SIZE + 256;
    char query[query_max_len];
    size_t query_len = snprintf(query, query_max_len,
        "UPDATE accounts SET pilot = '%s' WHERE xuid = CONV('%016lX', 16, 10);",
        pilot_escape, local_data->xuid);
    
    if (mysql_real_query(local_data->sqlcon, query, query_len)) {
        logmsg("Failed to update trade user info, got error: %s", mysql_error(local_data->sqlcon));
        trade_user_info.status = 1;
    }
    
    if (!mysql_affected_rows(local_data->sqlcon)) {
        logmsg("Did not update trade user info, zero rows affected");
        trade_user_info.status = 1;
    }
    
    logmsg("Trade user info response: %d delay", trade_user_info.delay);
    sendcmd(&trade_user_info);
}

struct req_trade_vt * decode_req_trade_vt(void * buf, struct vt_list ** rx_vts, struct vt_list ** tx_vts) {    
    // Compute offset to RX buffer
    void * rx_vts_ptr = buf + sizeof(struct req_trade_vt);
    *rx_vts = rx_vts_ptr;
    
    // Compute offset to TX buffer
    size_t rx_vts_len = sizeof(struct vt_list) + sizeof(struct vt_entry) * (*rx_vts)->len;
    *tx_vts = rx_vts_ptr + rx_vts_len;
    
    return buf;
}

void handle_trade_vt(void) {
    struct vt_list * rx_vts, * tx_vts;
    struct req_trade_vt * trade_vt_in = decode_req_trade_vt(local_data->recvbuf, &rx_vts, &tx_vts);
    logmsg("Request trade VT: Partner Pilot Name: \"%.16s\", Token %d, Rx VT Len %d, Tx VT Len %d",
        trade_vt_in->name, trade_vt_in->token, rx_vts->len, tx_vts->len);
    
    struct resp_vt_list * trade_vt = malloc(sizeof(struct resp_vt_list) + sizeof(struct vt_entry) * rx_vts->len);
    // Assume this is a failure until we've waited for our partner
    trade_vt->code = 0x2045;
    trade_vt->status = 1;
    trade_vt->vts.len = 0;
    trade_vt->len = COMMAND_LEN(resp_vt_list);
    
    bool success = wait_for_partner(trade_vt_in->code, trade_vt_in->token);
    
    if (!success) {
        sendcmd(trade_vt);
        free(trade_vt);
        return;
    }
    
    for (int i = 0; i < rx_vts->len; i++) {
        uint16_t type = rx_vts->list[i].type;
        uint32_t serial = rx_vts->list[i].serial;
        
        char query[256];
        snprintf(query, sizeof(query),
            "UPDATE garage SET sortied = 0, faction = %d, xuid = CONV('%016lX', 16, 10) WHERE type = %d AND serial = %d;",
            local_data->faction, local_data->xuid, type, serial);
        
        if (mysql_query(local_data->sqlcon, query)) {
            logmsg("Failed to add traded VT to garage, got error: %s", mysql_error(local_data->sqlcon));
            client_handler_exit(1);
        }
        
        if (mysql_affected_rows(local_data->sqlcon)) {
            uint16_t pos = trade_vt->vts.len;
            trade_vt->vts.list[pos].type = type;
            trade_vt->vts.list[pos].serial = serial;
            trade_vt->vts.len++;
        } else {
            logmsg("Failed to add traded VT to garage, no affected rows: Type %d, Serial %010d", type, serial);
        }
    }
    
    trade_vt->status = 0;
    trade_vt->len = COMMAND_LEN(resp_vt_list) + sizeof(struct vt_entry) * trade_vt->vts.len;
    
    logmsg("Trade VT response: %d VTs recieved", trade_vt->vts.len);
    sendcmd(trade_vt);
    free(trade_vt);
}

void * client_handler(void * client_fd) {
    mysql_thread_init();

    // Setup thread data

    local_data = calloc(1, sizeof(struct client_data));
    local_data->client_fd = (long)client_fd;
    local_data->tid = gettid();
    
    // Increment thread count
    pthread_mutex_lock(&client_mtx);
    client_count++;
    logmsg("Client connect: %d Clients, %ld FD", client_count, client_fd);
    pthread_mutex_unlock(&client_mtx);
    
    // Wait for response to liveness check
    sendcmd(&cmd_liveness);
    {
        struct command_base * liveness = recvcmd();
        if (!liveness) {
            logmsg("No liveness response recieved");
            client_handler_exit(1);
        } else if (liveness->code != 0x2010 || liveness->len) {
            logmsg("Invalid liveness response: code %04X, length %d", liveness->code, liveness->len);
            client_handler_exit(1);
        }
    }
    
    // Request the XUID for this request
    sendcmd(&cmd_xuidreq);
    {
        struct command_xuid * xuidreq = recvcmd();
        if (!xuidreq) {
            logmsg("No xuid response recieved");
            client_handler_exit(1);
        } else if (xuidreq->code != 0x2020 || xuidreq->len != COMMAND_LEN(command_xuid)) {
            logmsg("Invalid xuid response: code %04X, length %d", xuidreq->code, xuidreq->len);
            client_handler_exit(1);
        }
        local_data->xuid = xuidreq->xuid;
    }
    
    // We're now reasonably confident that this is an actual SB game talking with us
    
    // Initialize MySQL connection for this thread
    local_data->sqlcon = mysql_init(NULL);
    
    if (!mysql_real_connect(local_data->sqlcon, NULL, config.user, config.pass, config.schema, 0, NULL, 0)) {
        logmsg("Failed to connect to database, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }

    char query[256];
    snprintf(query, sizeof(query), "INSERT IGNORE INTO accounts SET xuid = CONV('%016lX', 16, 10);", local_data->xuid);

    // Create an entry for this user in the DB
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to insert account information, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    snprintf(query, sizeof(query), "SELECT xname, faction, language, banned FROM accounts WHERE xuid = CONV('%016lX', 16, 10);", local_data->xuid);
    if (mysql_query(local_data->sqlcon, query)) {
        logmsg("Failed to query account information, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(local_data->sqlcon))) {
        logmsg("Failed to get account information result, got error: %s", mysql_error(local_data->sqlcon));
        client_handler_exit(1);
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 4) {
        logmsg("Incorrect number of fields (%d) in account information result", num_fields);
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row) {
        logmsg("Failed to get row from account information result, got error: %s", mysql_error(local_data->sqlcon));
        mysql_free_result(sqlres);
        client_handler_exit(1);
    }
    
    // Record the player name
    if (row[0]) strncpy(local_data->xname, row[0], 16);

    // Set faction
    local_data->faction = strtoul(row[1], NULL, 10);
    
    // Set language
    local_data->language = row[2] ? strtoul(row[2], NULL, 10) : -1;

    // Record if this user is banned
    local_data->banned = strtoul(row[3], NULL, 10);
    
    mysql_free_result(sqlres);
    
    // Ask client to send it's request
    sendcmd(&cmd_connect);
    struct command_base * req;
    while (req = recvcmd()) {
        switch (req->code) {
        // War Management
        case 0x3030: handle_turn_days();            break;
        case 0x1030: handle_campaign_period();      break;
        case 0x1031: handle_war_tide();             break;
        case 0x1032: handle_war_influence();        break;
        case 0x1028: handle_round_points();         break;
        
        // Mission Management
        case 0x1090: handle_analyze();              break;
        case 0x5026: handle_add_user_point();       break;
        case 0x5027: handle_add_mission_point();    break;
        
        // Server MOTDs
        case 0x101A: handle_free_message();         break;
        case 0x101B: handle_fixed_message();        break;
        
        // Pilot Data
        case 0x1021: handle_get_user_info();        break;
        case 0x5022: handle_new_user_info();        break;
        case 0x5024: handle_set_user_info();        break;
        case 0x1023: handle_del_user_info();        break;
        case 0x5025: handle_set_user_faction();     break;
        
        // Garage Management
        case 0x1040: handle_vt_owned();             break;
        case 0x1042: handle_vt_add();               break;
        case 0x1044: handle_vt_del();               break;
        case 0x1046: handle_vt_capture();           break;
        case 0x1047: handle_vt_sortie();            break;
        case 0x3047: handle_vt_return();            break;

        // Shop
        case 0x1041: handle_shop_list();            break;
        case 0x1049: handle_vt_potential_cost();    break;
        
        // Message Board
        case 0x1050: handle_message_count();        break;
        case 0x1052: handle_message_post();         break;
        
        // Leaderboard
        case 0x1060: handle_rank_values();          break;
        case 0x1061: handle_rank_count_global();    break;
        case 0x1062: handle_rank_count_faction();   break;
        case 0x1063: handle_rank_count_categories();break;
        
        // Awards
        case 0x5041: handle_awards_available();     break;
        
        // Trading
        case 0x104A: handle_trade_token();          break;
        case 0x104B: handle_trade_user_info();      break;
        case 0x1045: handle_trade_vt();             break;
        
        // Unknown Command
        case 0x7041: handle_loc_unknown();          break;
        default:
            logmsg("Unknown request %04X, length %d", req->code, req->len);
            
            uint16_t buflen = local_data->recvbuf_len;
            if (buflen > 64); buflen = 64;
            
            uint8_t buffer[256];
            for (int i = 0; i < buflen; i++) {
                sprintf(buffer + (3 * i), " %02X", local_data->recvbuf[i]);
            }
            logmsg("Buffer:%s", buffer);

            client_handler_exit(1);
        }
    }
    
    client_handler_exit(0);
}

char freemsg_en[1024];
char freemsg_jp[1024];
char service_query[4096];
unsigned int service_handler(MYSQL * sqlcon) {
    if (mysql_query(sqlcon, "SELECT round, turn, UNIX_TIMESTAMP(period) - UNIX_TIMESTAMP(NOW()), period, freemsg_en, freemsg_jp FROM settings ORDER BY round DESC, turn DESC LIMIT 1;")) {
        fprintf(stderr, "Failed to query settings, got error: %s\n", mysql_error(sqlcon));
        return 60;
    }
    
    MYSQL_RES * sqlres;
    if (!(sqlres = mysql_store_result(sqlcon))) {
        fprintf(stderr, "Failed to get settings, got error: %s\n", mysql_error(sqlcon));
        return 60;
    }
    
    int num_fields = mysql_num_fields(sqlres);
    if (num_fields != 6) {
        fprintf(stderr, "Incorrect number of fields (%d) in settings result\n", num_fields);
        mysql_free_result(sqlres);
        return 60;
    }
    
    MYSQL_ROW row = mysql_fetch_row(sqlres);
    if (!row) {
        mysql_free_result(sqlres);
        printf("No settings entries, assuming fresh start\n");
    
        if (mysql_query(sqlcon, "INSERT INTO settings (round, turn, period) VALUES (1, 1, DATE_ADD(NOW(), INTERVAL 1 WEEK));")) {
            fprintf(stderr, "Failed to insert initial settings, got error: %s\n", mysql_error(sqlcon));
            return 60;
        }
    
        return TURN_SECONDS;
    }
    
    uint16_t round = strtoul(row[0], NULL, 10);
    uint16_t turn = strtoul(row[1], NULL, 10);
    int32_t remaining = strtol(row[2], NULL, 10);

    char period[32];
    strncpy(period, row[3], sizeof(period));
    
    if (row[4]) snprintf(freemsg_en, sizeof(freemsg_en), "\"%s\"", row[4]);
    else strncpy(freemsg_en, "null", sizeof(freemsg_en));
    
    if (row[5]) snprintf(freemsg_jp, sizeof(freemsg_en), "\"%s\"", row[5]);
    else strncpy(freemsg_jp, "null", sizeof(freemsg_jp));
    
    mysql_free_result(sqlres);
    
    // Wait for period to expire
    if (remaining > 0) return remaining + 1;

    if (mysql_query(sqlcon, "UPDATE shop SET vtstock = vtlimit;")) {
        fprintf(stderr, "Failed to update shop vtstock, got error: %s\n", mysql_error(sqlcon));
        return 60;
    }
    
    bool faction_reset = false;
    
    if (turn < 8) {
        turn++;
        
        if (turn == 3 || turn == 5) faction_reset = true;
    } else {
        // Reset to defaults for new round
        faction_reset = true;
        round++;
        turn = 1;
    }
    
    if (faction_reset) {
        if (mysql_query(sqlcon, "UPDATE accounts SET faction = 0;")) {
            fprintf(stderr, "Failed to remove faction affiliations, got error: %s\n", mysql_error(sqlcon));
        } else {
            int rows = mysql_affected_rows(sqlcon);
            printf("Removed %d faction affiliations\n", rows);
        }
    }

    snprintf(service_query, sizeof(service_query), // This needs to be the last call made
        "INSERT INTO settings (round, turn, period, freemsg_en, freemsg_jp) VALUES (%d, %d, DATE_ADD(\"%s\", INTERVAL 1 WEEK), %s, %s);",
        round, turn, period, freemsg_en, freemsg_jp);
    
    if (mysql_query(sqlcon, service_query)) {
        fprintf(stderr, "Failed to insert new settings, got error: %s\n", mysql_error(sqlcon));
        return 60;
    }

    return TURN_SECONDS;
}

void service_handle_cleanup(void * _) {
    printf("Service thread terminated\n");
    mysql_thread_end();
}

void * service_handler_setup(void * _) {
    int oldstate, oldtype;
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &oldtype);

    mysql_thread_init();
    
    pthread_cleanup_push(service_handle_cleanup, NULL);
    
    printf("Service thread started\n");
    
    while (true) {
        // Re-open this connection each time since this is called only once a week
        unsigned int delay;
        MYSQL * sqlcon = mysql_init(NULL);
        if (mysql_real_connect(sqlcon, NULL, config.user, config.pass, config.schema, 0, NULL, 0)) {
            printf("Executing service thread\n");
            delay = service_handler(sqlcon);

            mysql_close(sqlcon);
        } else {
            fprintf(stderr, "Failed to connect to database, got error: %s", mysql_error(sqlcon));
            delay = 60;
        }

        printf("Service thread waiting for %d seconds\n", delay);
        
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &oldstate);
        sleep(delay);
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);
    }
    
    // This is unreachable but still needs to be included
    pthread_cleanup_pop(1);
}

static volatile int running = 1;
int server_fd;

void sigterm_handler(int sig) {
    printf("SIGTERM received %d\n", sig);
    running = 0;
    close(server_fd);
}

int load_config(char * path, struct config_data * cf) {
    FILE * conf = fopen(path, "r");
    if (!conf) {
        fprintf(stderr, "Failed to open config file: %s\n", path);
        return 1;
    }
    
    cf->server_addr.sin_family = AF_INET;
    
    const char * whitespace = " \r\n";
    char line[MAX_CONFIG_LINE_LEN];

    cf->game = UnknownGame;
    
    fgets(line, MAX_CONFIG_LINE_LEN, conf);
    line[strcspn(line, whitespace)] = '\0';
    if (!strcmp(line, "Brainbox")) {
        cf->game = Brainbox;
    } else if (!strcmp(line, "PilotTest")) {
        cf->game = PilotTest;
    } else if (!strcmp(line, "LineOfContact")) {
        cf->game = LineOfContact;
    } else {
        fprintf(stderr, "Unknown game type: %s\n", line);
        fclose(conf);
        return 1;
    }
    
    fgets(line, MAX_CONFIG_LINE_LEN, conf);
    line[strcspn(line, whitespace)] = '\0';
    
    if (inet_pton(cf->server_addr.sin_family, line, &cf->server_addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid IP address: %s\n", line);
        fclose(conf);
        return 1;
    }
    
    fgets(line, MAX_CONFIG_LINE_LEN, conf);
    line[strcspn(line, whitespace)] = '\0';
    
    int server_port = strtoul(line, NULL, 10);
    if (!server_port) {
        fprintf(stderr, "Invalid port: %s\n", line);
        fclose(conf);
        return 1;
    }
    
    cf->server_addr.sin_port = htons(server_port);
    
    if (!fgets(cf->user, MAX_CONFIG_LINE_LEN, conf)) {
        fprintf(stderr, "Missing database username\n");
        fclose(conf);
        return 1;
    }
    cf->user[strcspn(cf->user, whitespace)] = '\0';
    
    if (!fgets(cf->pass, MAX_CONFIG_LINE_LEN, conf)) {
        fprintf(stderr, "Missing database password\n");
        fclose(conf);
        return 1;
    }    
    cf->pass[strcspn(cf->pass, whitespace)] = '\0';
    
    if (!fgets(cf->schema, MAX_CONFIG_LINE_LEN, conf)) {
        fprintf(stderr, "Missing database schema name\n");
        fclose(conf);
        return 1;
    }
    cf->schema[strcspn(cf->schema, whitespace)] = '\0';
    
    fclose(conf);
    return 0;
}

int main(int argc, char ** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <config.conf>\n", argv[0]);
        return 1;
    }
    
    // Try and load a config file
    if (load_config(argv[1], &config)) return 1;
    
    printf("Game type is set to: %s\n", GameType_string[config.game]);
    
    printf("MySQL client version: %s\n", mysql_get_client_info());
    if (mysql_library_init(0, NULL, NULL)) {
        fprintf(stderr, "Failed to initialize MySQL library\n");
        return 1;
    }
    
    if (pthread_mutex_init(&client_mtx, NULL) != 0) { 
        printf("Failed to initialize client_mtx\n"); 
        return 1; 
    }
    
    if (pthread_mutex_init(&trade_mtx, NULL) != 0) { 
        printf("Failed to initialize trade_mtx\n"); 
        return 1; 
    }

    server_fd = socket(config.server_addr.sin_family, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }
    
    int reuse_opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &reuse_opt, sizeof(reuse_opt))) {
        perror("setsockopt");
        return 1;
    }   
    
    if (bind(server_fd, (struct sockaddr*)&config.server_addr, sizeof(config.server_addr)) < 0) {
        perror("bind failed");
        return 1;
    }
    
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        return 1;
    }
    
    signal(SIGTERM, sigterm_handler);
    
    pthread_t service_thread;
    errno = pthread_create(&service_thread, NULL, service_handler_setup, NULL);
    if (errno) {
        perror("pthread create service");
        return 1;
    }
    
    char server_ip[INET_ADDRSTRLEN];
    inet_ntop(config.server_addr.sin_family, &config.server_addr.sin_addr, server_ip, INET_ADDRSTRLEN);
    
    printf("Campaign server started %s:%d\n", server_ip, ntohs(config.server_addr.sin_port));
    
    sd_notifyf(0, "READY=1\nSTATUS=Running\nMAINPID=%lu", (unsigned long) getpid());
    
    while(running) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(struct sockaddr_in);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_addr_len);
        if (client_fd < 0) {
            if (errno != EBADF) {
                // Don't show an error if terminated by CTRL + C
                perror("accept");
            }
            
            continue;
        }
        
        pthread_t client_thread;
        errno = pthread_create(&client_thread, NULL, client_handler, (void *)(long)client_fd);
        if (errno) {
            perror("pthread_create client");
            
            // Tell the XBOX to try again
            write(client_fd, &cmd_wait, sizeof(cmd_wait));
            close(client_fd);
            continue;
        }
        pthread_detach(client_thread);
    }

    sd_notify(0, "STOPPING=1\nSTATUS=Waiting for all clients to finish\n");
    
    // Terminate service thread
    pthread_cancel(service_thread);
    
    // Wait for all threads to terminate
    while (1) {
        pthread_mutex_lock(&client_mtx);
        int all_done = client_count <= 0;
        pthread_mutex_unlock(&client_mtx);
        if (all_done) break;
        printf("Remaining client connections %ld\n", client_count);
        sleep(1);
    }
    
    // Shutdown mysql library
    mysql_library_end();
    
    return 0;
}
