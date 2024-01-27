#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>

// code_unknown1 = 0x2010,
// code_unknown2 = 0x2020,

#define COMMAND_LEN(CMD_NAME) (sizeof(struct CMD_NAME) - 4)

struct command_base {
    uint16_t len;
    uint16_t code;
} __attribute__((packed));

struct command_arg1 {
    uint16_t len;
    uint16_t code;
    uint8_t arg;
} __attribute__((packed));

struct command_arg1a {
    uint16_t len;
    uint16_t code;
    uint8_t arg[8];
} __attribute__((packed));

struct command_arg2 {
    uint16_t len;
    uint16_t code;
    uint16_t arg;
} __attribute__((packed));

struct command_arg2a {
    uint16_t len;
    uint16_t code;
    uint16_t arg[8];
} __attribute__((packed));

struct command_arg4 {
    uint16_t len;
    uint16_t code;
    uint32_t arg;
} __attribute__((packed));

struct command_xuid {
    uint16_t len;
    uint16_t code;
    uint64_t xuid;
} __attribute__((packed));

struct command_period {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint32_t second;
} __attribute__((packed));

struct command_freemess {
    uint16_t len;
    uint16_t code;
    uint16_t retVal;
    uint16_t msgLen;
    uint8_t msg[256];
} __attribute__((packed));

struct command_fixedmess {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t msgLen;
    uint8_t msg[256];
} __attribute__((packed));

struct command_potentialVTCost {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t vtCostsLen;
    struct {
        uint16_t id;
        uint16_t cost;
    } vtCosts[32];
} __attribute__((packed));

#define PILOT_DATA_SIZE 1819
#define PILOT_NAME_LEN 16
#define VT_LIST_SIZE 60

struct command_userInfo {
    uint16_t len;
    uint16_t code;
    uint8_t retVal; // 0=Registered, 1=Unregistered
    uint8_t name[PILOT_NAME_LEN];
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct command_userRegist {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    uint8_t xliveName[PILOT_NAME_LEN];
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct command_userUpdate {
    uint16_t len;
    uint16_t code;
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct vt_entry {
    uint16_t id;
    uint32_t serial;
} __attribute__((packed));

struct vt_list {
    uint16_t len;
    struct vt_entry list[VT_LIST_SIZE];
} __attribute__((packed));

struct command_vtGet {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    struct vt_list vts;
} __attribute__((packed));

struct command_warInf {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint32_t padding;
    // These 3 values must add up to 100
    uint32_t hsd;
    uint32_t prf;
    uint32_t rb;
} __attribute__((packed));

struct command_vtGettable {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t vtListLen;
    struct {
        uint16_t id;
        uint16_t limit; // Purely visual, doesn't affect the ability to buy one
        uint16_t a;
        uint16_t cost;
        uint8_t padding;
    } __attribute__((packed)) vtList[32];
} __attribute__((packed));

struct command_vtPurchase {
    uint16_t len;
    uint16_t code;
    uint16_t vtListLen;
    uint16_t vtList[VT_LIST_SIZE];
} __attribute__((packed));

struct command_vtThrow {
    uint16_t len;
    uint16_t code;
    struct vt_list vts;
} __attribute__((packed));

struct prizeall_entry {
    uint16_t round;
    uint16_t turn;
    uint32_t c;
    uint16_t d;
    uint32_t e;
} __attribute__((packed));

struct prize_entry {
    uint32_t a;
    uint16_t vt_id;
    uint32_t c;
} __attribute__((packed));

#define PRIZE_LIST_LEN 32

struct command_prizeableAll {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t prizeLen;
    struct prizeall_entry prizes[PRIZE_LIST_LEN];
} __attribute__((packed));

struct command_prizePoints {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint8_t padding;
    uint16_t pointLen;
    uint32_t points[PRIZE_LIST_LEN];
} __attribute__((packed));

struct command_prizes {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint32_t padding;
    uint16_t prizeLen;
    struct prize_entry prizes[PRIZE_LIST_LEN];
} __attribute__((packed));

struct command_rankInd {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint8_t padding0;
    uint32_t padding1;
    uint32_t a;
    uint32_t padding2;
    uint32_t b;
} __attribute__((packed));

struct rankind_entry {
    uint8_t a;
    uint32_t b;
    uint32_t c;
    uint8_t padding;
}  __attribute__((packed));

struct command_rankIndAll {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint32_t padding;
    uint16_t rankLen;
    struct rankind_entry ranks[32];
} __attribute__((packed));

struct command_rankWarReq {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t faction;
    uint8_t category;
    uint32_t a;
    uint16_t b;
} __attribute__((packed));

struct rankwar_entry {
    uint32_t rank;
    uint8_t name[PILOT_NAME_LEN];
    uint8_t xliveName[PILOT_NAME_LEN];
    uint32_t a;
    uint32_t value;
} __attribute__((packed));

struct command_rankWarResp {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint8_t padding[6];
    uint32_t a;
    uint16_t rankLen;
    struct rankwar_entry ranks[32];
} __attribute__((packed));

struct command_vtPrizeReq {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint16_t prize; // 1 = Left Option, 0 = Right Option
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct command_vtPrizeResp {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint32_t padding;
    struct vt_list vts;
} __attribute__((packed));

struct command_warTide {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint8_t padding[5];
    // These three values must sum to 100
    uint32_t hsd;
    uint32_t prf;
    uint32_t rb;
} __attribute__((packed));

struct command_vtDeposit {
    uint16_t len;
    uint16_t code;
    struct vt_list vts;
} __attribute__((packed));

struct command_missionPointAdd {
    uint16_t len;
    uint16_t code;
    uint16_t a;
    uint16_t b;
    uint8_t c;
    uint32_t d;
    uint8_t e;
} __attribute__((packed));

struct command_userPointAdd {
    uint16_t len;
    uint16_t code;
    uint16_t round;
    uint16_t turn;
    uint8_t map_number;
    uint32_t points[10];
} __attribute__((packed));

struct command_vtSeizeReq {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    struct vt_list vts;
} __attribute__((packed));

struct command_vtSeizeResp {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint8_t name[PILOT_NAME_LEN];
    struct vt_list vts;
} __attribute__((packed));

struct command_exchangeTokenReq {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
} __attribute__((packed));

struct command_exchangeTokenResp {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint32_t token;
} __attribute__((packed));

struct command_exchangeUserUpdateReq {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    uint32_t token;
    uint8_t data[PILOT_DATA_SIZE];
} __attribute__((packed));

struct command_exchangeUserUpdateResp {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t delay;
} __attribute__((packed));

struct command_exchangeVT {
    uint16_t len;
    uint16_t code;
    uint32_t token;
    uint8_t name[PILOT_NAME_LEN];
    struct vt_list vts;
    // There's a second vt_list after this one, get it dynamically
} __attribute__((packed));

struct command_exchangeUserConfirm {
    uint16_t len;
    uint16_t code;
    uint8_t name[PILOT_NAME_LEN];
    uint32_t token;
} __attribute__((packed));

struct command_boardMessNum {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t padding;
    uint32_t start;
    uint16_t count;
} __attribute__((packed));

struct command_boardMessRegist {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint16_t padding;
    uint32_t id;
} __attribute__((packed));

struct command_supplyPoint {
    uint16_t len;
    uint16_t code;
    uint8_t retVal;
    uint32_t points;
} __attribute__((packed));

struct command_base cmd_liveness = {COMMAND_LEN(command_base), 0x1010};
struct command_base cmd_xuidreq = {COMMAND_LEN(command_base), 0x1020};
struct command_arg4 cmd_connect = {COMMAND_LEN(command_arg4), 0x3013};
struct command_arg4 cmd_reject = {COMMAND_LEN(command_arg4), 0x3014};
struct command_arg4 cmd_wait = {COMMAND_LEN(command_arg4), 0x3015};

struct command_period cmd_period = {COMMAND_LEN(command_period), 0x2030};
struct command_freemess cmd_freemess = {.code = 0x201A};
struct command_fixedmess cmd_fixedmess = {.code = 0x201B};
struct command_potentialVTCost cmd_vtpotentialcost = {.code = 0x2049};

struct command_userInfo cmd_userinfo = {COMMAND_LEN(command_userInfo), .code = 0x2021};
struct command_arg1 cmd_userregist = {COMMAND_LEN(command_arg1), 0x6022};
struct command_arg1 cmd_userupdate = {COMMAND_LEN(command_arg1), 0x6024};
struct command_arg1 cmd_userentry = {COMMAND_LEN(command_arg1), 0x6025};
struct command_arg1 cmd_userdelete = {COMMAND_LEN(command_arg1), 0x2023};

struct command_vtGet cmd_vtmine = {.code = 0x2040};
struct command_vtGet cmd_vtget = {.code = 0x2042};
struct command_vtGet cmd_vtthrow = {.code = 0x2044};

struct command_warInf cmd_warinf = {COMMAND_LEN(command_warInf), 0x2032};

struct command_vtGettable cmd_vtpurchasable = {.code = 0x2041};
struct command_prizeableAll cmd_prizeableall = {.code = 0x6041};
struct command_prizePoints cmd_prizepoints = {.code = 0x204C};
struct command_prizes cmd_prizes = {.code = 0x4041};
struct command_vtPrizeResp cmd_claimprize = {.code = 0x2048};

struct command_rankIndAll cmd_rankindall = {.code = 0x4061};
struct command_rankInd cmd_rankind = {COMMAND_LEN(command_rankInd), 0x2061};
struct command_rankWarResp cmd_rankwar = {.code = 0x2060};

struct command_warTide cmd_wartide = {COMMAND_LEN(command_warTide), 0x2031};

struct command_arg1 cmd_vtdeposit = {COMMAND_LEN(command_arg1), 0x2047};
struct command_base cmd_analyze = {COMMAND_LEN(command_base), 0x2090};

struct command_arg1 cmd_userpointadd = {COMMAND_LEN(command_arg1), 0x6026};
struct command_arg1 cmd_missionpointadd = {COMMAND_LEN(command_arg1), 0x6027};
struct command_vtGet cmd_vtreturn = {.code = 0x4047};
struct command_vtSeizeResp cmd_vtseize = {.code = 0x2046};

struct command_exchangeTokenResp cmd_exchangetoken = {COMMAND_LEN(command_exchangeTokenResp), 0x204A};
struct command_exchangeUserUpdateResp cmd_exchangeuserupdate = {COMMAND_LEN(command_exchangeUserUpdateResp), 0x204B};
struct command_vtGet cmd_exchangevt = {.code = 0x2045};
struct command_arg1 cmd_exchangeuserconfirm = {COMMAND_LEN(command_arg1), 0x404B};

struct command_boardMessNum cmd_boardmessnum = {COMMAND_LEN(command_boardMessNum), 0x2050, 1};
struct command_boardMessRegist cmd_boardmessregist = {COMMAND_LEN(command_boardMessRegist), 0x2052, 1};
struct command_supplyPoint cmd_supplypoint = {COMMAND_LEN(command_supplyPoint), 0x2028};

// Pilot Test
struct command_arg2 cmd_ptunk = {COMMAND_LEN(command_arg2), 0x4030};

uint16_t encode_msg(uint8_t * dest, char * msg, uint16_t len) {
    uint16_t msglen = strlen(msg);
    len /= sizeof(uint16_t);
    if (msglen > len) msglen = len;
    uint16_t buf[msglen];
    for (int i = 0; i < msglen; i++) {
        buf[i] = msg[i];
    }
    msglen *= sizeof(uint16_t);
    memcpy(dest, buf, msglen);
    return msglen;
}

void send_command(int client, void * _cmd) {
    struct command_base * cmd = _cmd;
    send(client, cmd, cmd->len + 4, 0);
    printf("Sent command: %04X, size: %d\n", cmd->code, cmd->len);
}

void send_command_freemess(int client, char * msg) {
    cmd_freemess.msgLen = encode_msg(cmd_freemess.msg, msg, sizeof(cmd_freemess.msg));
    cmd_freemess.len = cmd_freemess.msgLen + 4;
    send_command(client, &cmd_freemess);
}

void send_command_fixedmess(int client) {
    cmd_fixedmess.len = cmd_fixedmess.msgLen + 3;
    send_command(client, &cmd_fixedmess);
}

void send_command_vtpotentialcost(int client) {
    cmd_vtpotentialcost.len = (cmd_vtpotentialcost.vtCostsLen * 4) + 3;
    send_command(client, &cmd_vtpotentialcost);
}

void send_command_userinfo(int client, bool isUnregistered) {
    cmd_userinfo.retVal = isUnregistered ? 2 : 0;
    send_command(client, &cmd_userinfo);
}

void send_command_userregist(int client, uint8_t ret) {
    cmd_userregist.arg = ret;
    send_command(client, &cmd_userregist);
}

void send_command_vtget(int client, struct command_vtGet * vtg) {
    vtg->len = (vtg->vts.len * sizeof(struct vt_entry)) + 3;
    send_command(client, vtg);
}

void send_command_warinf(int client, uint8_t ret) {
    cmd_warinf.retVal = ret;
    send_command(client, &cmd_warinf);
}

void send_command_vtpurchasable(int client) {
    cmd_vtpurchasable.len = (cmd_vtpurchasable.vtListLen * sizeof(*cmd_vtpurchasable.vtList)) + 3;
    send_command(client, &cmd_vtpurchasable);
}

void send_command_prizeableall(int client) {
    cmd_prizeableall.len = (cmd_prizeableall.prizeLen * sizeof(struct prizeall_entry)) + 3;
    send_command(client, &cmd_prizeableall);
}

void send_command_prizepoints(int client) {
    cmd_prizepoints.len = (cmd_prizepoints.pointLen * sizeof(uint32_t)) + 4;
    send_command(client, &cmd_prizepoints);
}

void send_command_prizes(int client) {
    cmd_prizes.len = (cmd_prizes.prizeLen * sizeof(struct prize_entry)) + 7;
    send_command(client, &cmd_prizes);
}

void send_command_rankwar(int client) {
    cmd_rankwar.len = (cmd_rankwar.rankLen * sizeof(struct rankwar_entry)) + 13;
    send_command(client, &cmd_rankwar);
}

void send_commmand_rankindall(int client) {
    cmd_rankindall.len = (cmd_rankindall.rankLen * sizeof(struct rankind_entry)) + 7;
    send_command(client, &cmd_rankindall);
}

void send_command_claimprize(int client) {
    cmd_claimprize.len = (cmd_claimprize.vts.len * sizeof(struct vt_entry)) + 3;
    send_command(client, &cmd_claimprize);
}

void send_command_vtseize(int client) {
    cmd_vtseize.len = (cmd_vtseize.vts.len * sizeof(struct vt_entry)) + 17;
    send_command(client, &cmd_vtseize);
}

struct vt_list * get_exchange_vtlist(struct command_exchangeVT * cmd) {
    void * vts_pos = &cmd->vts;
    vts_pos += sizeof(uint16_t) + (cmd->vts.len * sizeof(struct vt_entry));
    return vts_pos;
}

uint8_t recvbuf[5124];
uint32_t vt_serials[32];

int main(int argc, char ** argv) {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0) {
        perror("socket");
        return 1;
    }
    
    int opt = 1;
    if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        return 1;
    }
    
    struct sockaddr_in server_addr;
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(23456);
    
    if (bind(server, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        return 1;
    }
    
    if (listen(server, 3) < 0) {
        perror("listen");
        return 1;
    }
    
    printf("Campaign server started\n");

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(struct sockaddr_in);
        int client = accept(server, (struct sockaddr*)&client_addr, &client_addr_len);
        if (client < 0) {
            perror("accept");
            return 1;
        }

        char client_addr_str[INET_ADDRSTRLEN];
        printf("New connection from: %s\n", inet_ntop(AF_INET, &(client_addr.sin_addr), client_addr_str, INET_ADDRSTRLEN));

        send_command(client, &cmd_liveness);
        
        uint64_t xuid;
        int running = 1;
        while (running) {
            int recv_len = recv(client, recvbuf, sizeof(recvbuf), 0);
            if (recv_len < 0) {
                break;
            } else if (!recv_len) continue;
            //printf("Recieve length: %d\n", recv_len);
            
            void * cmd_in_raw = recvbuf;
            
            struct command_base * cmd_in_base = cmd_in_raw;
            char pdata_path[256];
            FILE * pdata;
            
            struct vt_list garage = {0};
            
            running = 0;
            switch (cmd_in_base->code) {
            case 0x2010: // Liveness response
                send_command(client, &cmd_xuidreq);
                running = 1;
                break;
            case 0x2020:; // XUID response
                struct command_xuid * cmd_in_xuid = cmd_in_raw;
                printf("XUID: %016lX\n", cmd_in_xuid->xuid);
                xuid = cmd_in_xuid->xuid;
                
                cmd_connect.arg = 1;
                send_command(client, &cmd_connect);
                running = 1;
                break;
            // Actual server requests
            case 0x1030: // Get Campaign Period
                cmd_period.round = 1;
                cmd_period.turn = 1;
                cmd_period.second = 7 * 86400 * 10; // 1 week left
                send_command(client, &cmd_period);
                break;
            case 0x101A:; // Get Free Message
                struct command_arg1 * cmd_in_freemess = cmd_in_raw;
                printf("Language: %s\n", cmd_in_freemess->arg ? "English" : "Japanese");
                send_command_freemess(client, "");// "We're fucking back baby!");
                break;
            case 0x101B: // Get Fixed Message
                cmd_fixedmess.msg[0] = 2;
                cmd_fixedmess.msg[1] = 3;
                cmd_fixedmess.msg[2] = 8;
//                cmd_fixedmess.msg[3] = 16;
                cmd_fixedmess.msgLen = 3;
                send_command_fixedmess(client);
                break;
            case 0x1049: // Get VT Potential Cost
                cmd_vtpotentialcost.vtCosts[0].id = 0;
                cmd_vtpotentialcost.vtCosts[0].cost = 2;
                cmd_vtpotentialcost.vtCostsLen = 1;
                send_command_vtpotentialcost(client);
                break;
            case 0x1021: // Get User Info                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "rb");
                if (pdata) {
                    fseek(pdata, 0x2B, SEEK_SET);
                    fread(cmd_userinfo.name, sizeof(uint8_t), PILOT_NAME_LEN, pdata);
                    printf("Pilot name: %.16s\n", cmd_userinfo.name);
                    fseek(pdata, 0, SEEK_SET);
                    fread(cmd_userinfo.data, sizeof(uint8_t), PILOT_DATA_SIZE, pdata);
                    fclose(pdata);
                };
                
                send_command_userinfo(client, !pdata);
                break;
            case 0x5022:; // Register New User
                struct command_userRegist * cmd_in_userregist = cmd_in_raw;
                printf("New registration from XLive User: %.16s, pilot: %.16s\n", cmd_in_userregist->xliveName, cmd_in_userregist->name);
                
                snprintf(pdata_path, 256, "data/%016lX", xuid);
                
                struct stat st = {0};
                if (stat(pdata_path, &st) == -1) mkdir(pdata_path, 0700);
                
                // Record the Xbox Live name of the person who's registering this account
                snprintf(pdata_path, 256, "data/%016lX/xlive.data", xuid);
                pdata = fopen(pdata_path, "wb");
                if (pdata) {
                    fwrite(cmd_in_userregist->xliveName, sizeof(uint8_t), PILOT_NAME_LEN, pdata);
                    fclose(pdata);
                }
                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "wb");
                if (pdata) {
                    fwrite(cmd_in_userregist->data, sizeof(uint8_t), PILOT_DATA_SIZE, pdata);
                    fclose(pdata);
                }
                
                send_command_userregist(client, 0);
                break;
            case 0x5024:; // Update user profile
                struct command_userUpdate * cmd_in_userupdate = cmd_in_raw;
                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fseek(pdata, 1, SEEK_SET);
                    fwrite(cmd_in_userupdate->data, sizeof(uint8_t), PILOT_DATA_SIZE, pdata);
                    fclose(pdata);
                }
                
                send_command(client, &cmd_userupdate);
                break;
            case 0x5025: // Change faction
                struct command_arg1 * cmd_in_userEntry = cmd_in_raw;
                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fwrite(&cmd_in_userEntry->arg, sizeof(uint8_t), 1, pdata);
                    fclose(pdata);
                }
                
                send_command(client, &cmd_userentry);
                break;
            case 0x1023: // Delete user profile
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                unlink(pdata_path);
                
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                unlink(pdata_path);
                
                send_command(client, &cmd_userdelete);
                break;
            case 0x1040: // Request owned VTs (VT Mine)
                cmd_vtmine.vts.len = 0;
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb");
                if (pdata) {
                    fread(&cmd_vtmine.vts, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_vtget(client, &cmd_vtmine);
                break;
            case 0x1032:; // Request War Influence
                struct command_arg2a * cmd_in_warinf = cmd_in_raw;
                printf("War influence request args: %d %d\n", cmd_in_warinf->arg[0], cmd_in_warinf->arg[1]);
                cmd_warinf.hsd = 50;
                cmd_warinf.prf = 50;
                cmd_warinf.rb = 0;
                send_command_warinf(client, 0);
                break;
            case 0x1041: // Request available VTs in the store
                for (int i = 0; i < 32; i++) {
                    cmd_vtpurchasable.vtList[i].id = i;
                    cmd_vtpurchasable.vtList[i].limit = 100;
                    cmd_vtpurchasable.vtList[i].a = 1;
                    cmd_vtpurchasable.vtList[i].cost = 1;
                }
                
                cmd_vtpurchasable.vtListLen = 32;
                send_command_vtpurchasable(client);
                break;
            case 0x1042: // Purchase VT
                struct command_vtPurchase * cmd_in_vtpurchase = cmd_in_raw;
                
                cmd_vtget.vts.len = 0;
                
                // Read old VT data
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb");
                if (pdata) {
                    fread(&garage, sizeof(struct vt_list), 1, pdata);                    
                    fclose(pdata);
                }
                
                // Write new VT data
                pdata = fopen(pdata_path, "wb");
                if (pdata) {
                    for (int i = 0; i < cmd_in_vtpurchase->vtListLen; i++) {
                        uint16_t id = cmd_in_vtpurchase->vtList[i];
                        uint32_t serial = ++vt_serials[id];
                        
                        cmd_vtget.vts.list[i].id = id;
                        cmd_vtget.vts.list[i].serial = serial;
                        cmd_vtget.vts.len++;
                        
                        if (garage.len < VT_LIST_SIZE) {
                            uint16_t gi = garage.len++;
                            garage.list[gi].id = id;
                            garage.list[gi].serial = serial;
                        } else {
                            printf("Could not add VT, garage is full!\n");
                        }
                    }
                    
                    fwrite(&garage, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_vtget(client, &cmd_vtget);
                break;
            case 0x1044: // Discard VT
                struct command_vtThrow * cmd_in_vtthrow = cmd_in_raw;
                
                cmd_vtthrow.vts.len = 0;
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fread(&garage, sizeof(struct vt_list), 1, pdata);

                    printf("VT count %d\n", garage.len);
                    for (int i = 0; i < cmd_in_vtthrow->vts.len; i++) {
                        uint16_t id = cmd_in_vtthrow->vts.list[i].id;
                        uint32_t serial = cmd_in_vtthrow->vts.list[i].serial;
                        printf("XUser %016lX is discarding VT %d, serial %010d\n", xuid, id, serial);
                        
                        
                        for (int j = 0; j < garage.len; j++) {
                            // Find a VT with the same ID and Serial
                            if (garage.list[j].id == id &&  garage.list[j].serial == serial) {
                                // Remove the VT by overwriting it with the one at the end of the list
                                uint16_t garage_tail = --garage.len;
                                garage.list[j].id = garage.list[garage_tail].id;
                                garage.list[j].serial = garage.list[garage_tail].serial;
                                
                                // For some reason the output of this should ALWYAS be empty
                                // If any VT is present in the list then the game will report a failure
//                                uint16_t throw_tail = ++cmd_vtthrow.vts.len;
//                                cmd_vtthrow.vts.list[throw_tail].id = id;
//                                cmd_vtthrow.vts.list[throw_tail].serial = serial;
                                printf("VT Discarded\n");
                                break;
                            }
                        }
                    }
                    
                    printf("VT count %d\n", garage.len);
                    
                    fseek(pdata, 0, SEEK_SET);
                    fwrite(&garage, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_vtget(client, &cmd_vtthrow);
                break;
            case 0x5041: // Request Prize list
                cmd_prizeableall.prizes[0].round = 4;
                cmd_prizeableall.prizes[0].turn = 3;
                // These don't matter
                cmd_prizeableall.prizes[0].c = 0;
                cmd_prizeableall.prizes[0].d = 0;
                cmd_prizeableall.prizes[0].e = 0;
                
                cmd_prizeableall.prizeLen = 1; 
            
                send_command_prizeableall(client);
                break;
            case 0x3041: // Request Prize
                struct command_arg2a * cmd_in_prize = cmd_in_raw;
                printf("Request prize args: Round %d, Turn %d\n", cmd_in_prize->arg[0], cmd_in_prize->arg[1]);
                
                cmd_prizes.prizes[0].a = 0; // Doesn't matter
                cmd_prizes.prizes[0].vt_id = 7; // If this option is greater than 34 then no VT will be provided
                cmd_prizes.prizes[0].c = 0;
                cmd_prizes.prizeLen = 1;
                
                send_command_prizes(client);
                break;
            case 0x104C: // Request Prize Point
                struct command_arg1 * cmd_in_prizepoint = cmd_in_raw;
                printf("Request prize point: %d\n", cmd_in_prizepoint->arg);
                
                for (int i = 0; i < 8; i++) cmd_prizepoints.points[i] = cmd_in_prizepoint->arg ? 100000 : 300000;
                cmd_prizepoints.pointLen = 8;
                
                send_command_prizepoints(client);
                break;
            case 0x3061:; // Request rank index all
                struct command_arg2a * cmd_in_rankindall = cmd_in_raw;
                printf("Request rank index all args: Round %d, Turn %d\n", cmd_in_rankindall->arg[0], cmd_in_rankindall->arg[1]);

                cmd_rankindall.ranks[0].a = 1; // Does something when 1, 3, 5, or 6? (Doesn't matter which value, does the same thing for any of them)
                cmd_rankindall.ranks[0].b = 1;
                cmd_rankindall.ranks[0].c = 0; // Doesn't matter
                
                cmd_rankindall.ranks[1].a = 3;
                cmd_rankindall.ranks[1].b = 1;
                cmd_rankindall.ranks[1].c = 0;
                
                cmd_rankindall.rankLen = 2;
                send_commmand_rankindall(client);
                break;
            case 0x1061: // Request rank index
                struct command_arg2a * cmd_in_rankind = cmd_in_raw;
                printf("Request rank index: %d %d %d\n", cmd_in_rankind->arg[0], cmd_in_rankind->arg[1], cmd_in_rankind->arg[2]);
            
                cmd_rankind.a = 1;
                cmd_rankind.b = 2;
                send_command(client, &cmd_rankind);
                break;
            case 0x1060: // Request rank war
                struct command_rankWarReq * cmd_in_rankwar = cmd_in_raw;
                printf("Request rank war for Round %d, Turn %d, Faction %d, Category %d, A %d, B %d\n",
                    cmd_in_rankwar->round, cmd_in_rankwar->turn, cmd_in_rankwar->faction,
                    cmd_in_rankwar->category, cmd_in_rankwar->a, cmd_in_rankwar->b);
                
                cmd_rankwar.a = 3;
                
                cmd_rankwar.ranks[0].rank = 1;
                cmd_rankwar.ranks[0].a = 3;
                cmd_rankwar.ranks[0].value = 4;
                
                snprintf(pdata_path, 256, "data/%016lX/xlive.data", xuid);
                pdata = fopen(pdata_path, "rb");
                if (pdata) {
                    fread(cmd_rankwar.ranks[0].xliveName, sizeof(uint8_t), PILOT_NAME_LEN, pdata);
                    fclose(pdata);
                }
                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "rb");
                if (pdata) {
                    fseek(pdata, 0x2B, SEEK_SET);
                    fread(cmd_rankwar.ranks[0].name, sizeof(uint8_t), PILOT_NAME_LEN, pdata);
                    fclose(pdata);
                }
                
                cmd_rankwar.rankLen = 1;
                send_command_rankwar(client);
                break;
            case 0x1048: // Claim prize
                struct command_vtPrizeReq * cmd_in_vtprize = cmd_in_raw;
                printf("VT Prize claim, Round %d, Turn %d, Option: %s\n", cmd_in_vtprize->round, cmd_in_vtprize->turn, cmd_in_vtprize->prize ? "Points + VT" : "Points Only");
                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fseek(pdata, 1, SEEK_SET);
                    fwrite(cmd_in_vtprize->data, sizeof(uint8_t), PILOT_DATA_SIZE, pdata);
                    fclose(pdata);
                }
                
                uint16_t id = 7;
                uint32_t serial = ++vt_serials[id];
                
                // This doesn't actually matter? It seems to check owned VTs anyway
                cmd_claimprize.vts.list[0].id = id;
                cmd_claimprize.vts.list[0].serial = serial;
                cmd_claimprize.vts.len = 1;
                
                // Read old VT data
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb");
                if (pdata) {
                    fread(&garage, sizeof(struct vt_list), 1, pdata);      
                    fclose(pdata);
                }
                
                uint16_t gi = garage.len++;
                garage.list[gi].id = id;
                garage.list[gi].serial = serial;
                
                // Write new VT data
                pdata = fopen(pdata_path, "wb");
                if (pdata) {
                    fwrite(&garage, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_claimprize(client);
                break;
            case 0x1031: // Get war tide
                struct command_arg2a * cmd_in_wartide = cmd_in_raw;
                printf("War tide args: %d %d %d\n", cmd_in_wartide->arg[0], cmd_in_wartide->arg[1], cmd_in_wartide->arg[2]);
                
                cmd_wartide.hsd = 50;
                cmd_wartide.prf = 50;
                
                send_command(client, &cmd_wartide);
                break;
            case 0x1047: // VT Deposit
                struct command_vtDeposit * cmd_in_vtdeposit = cmd_in_raw;
                
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fread(&garage, sizeof(struct vt_list), 1, pdata);

                    printf("VT count %d\n", garage.len);
                    for (int i = 0; i < cmd_in_vtdeposit->vts.len; i++) {
                        uint16_t id = cmd_in_vtdeposit->vts.list[i].id;
                        uint32_t serial = cmd_in_vtdeposit->vts.list[i].serial;
                        printf("XUser %016lX is depositing VT %d, serial %010d\n", xuid, id, serial);
                        
                        
                        for (int j = 0; j < garage.len; j++) {
                            // Find a VT with the same ID and Serial
                            if (garage.list[j].id == id &&  garage.list[j].serial == serial) {
                                // Remove the VT by overwriting it with the one at the end of the list
                                uint16_t garage_tail = --garage.len;
                                garage.list[j].id = garage.list[garage_tail].id;
                                garage.list[j].serial = garage.list[garage_tail].serial;
                                
                                printf("VT Deposited\n");
                                break;
                            }
                        }
                    }
                    
                    printf("VT count %d\n", garage.len);
                    
                    fseek(pdata, 0, SEEK_SET);
                    fwrite(&garage, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command(client, &cmd_vtdeposit);
                break;
            case 0x1090: // Analyze request
                // TODO: Figure out what this data actually means
            
                send_command(client, &cmd_analyze);
                break;
            case 0x5027: // Mission Point Add
                struct command_missionPointAdd * cmd_in_missionpointadd = cmd_in_raw;
                printf("Mission Point Add args: %d %d %d %d %d\n",
                    cmd_in_missionpointadd->a, cmd_in_missionpointadd->b,
                    cmd_in_missionpointadd->c, cmd_in_missionpointadd->d, cmd_in_missionpointadd->e);
                    
                send_command(client, &cmd_missionpointadd);
                break;
            case 0x5026: // User Point Add
                struct command_userPointAdd * cmd_in_userpointadd = cmd_in_raw;
                printf("User Point Add: Round %d, Turn, %d, Map Number %d\n", cmd_in_userpointadd->round, cmd_in_userpointadd->turn, cmd_in_userpointadd->map_number);
                for (int i = 0; i < 10; i++) {
                    printf("Point %d: %d\n", i + 1, cmd_in_userpointadd->points[i]);
                }
                
                send_command(client, &cmd_userpointadd);
                break;
            case 0x3047: // VT Return
                struct command_vtDeposit * cmd_in_vtreturn = cmd_in_raw;
                
                cmd_vtreturn.vts.len = 0;
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fread(&cmd_vtreturn.vts, sizeof(struct vt_list), 1, pdata);

                    printf("VT count %d\n", cmd_vtreturn.vts.len);
                    for (int i = 0; i < cmd_in_vtreturn->vts.len; i++) {
                        uint16_t id = cmd_in_vtreturn->vts.list[i].id;
                        uint32_t serial = cmd_in_vtreturn->vts.list[i].serial;
                        printf("XUser %016lX is returning VT %d, serial %010d\n", xuid, id, serial);
                        
                        uint16_t gi = cmd_vtreturn.vts.len++;
                        cmd_vtreturn.vts.list[gi].id = id;
                        cmd_vtreturn.vts.list[gi].serial = serial;
                    }
                    
                    printf("VT count %d\n", cmd_vtreturn.vts.len);
                    
                    fseek(pdata, 0, SEEK_SET);
                    fwrite(&cmd_vtreturn.vts, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_vtget(client, &cmd_vtreturn);
                break;
            case 0x1046: // VT Seize
                struct command_vtSeizeReq * cmd_in_vtseize = cmd_in_raw;
                printf("VT Seize Name %.16s\n", cmd_in_vtseize->name);
                
                memcpy(cmd_vtseize.name, cmd_in_vtseize->name, PILOT_NAME_LEN);
                
                cmd_vtseize.vts.len = 0;
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fread(&cmd_vtseize.vts, sizeof(struct vt_list), 1, pdata);

                    printf("VT count %d\n", cmd_vtseize.vts.len);
                    for (int i = 0; i < cmd_in_vtseize->vts.len; i++) {
                        uint16_t id = cmd_in_vtseize->vts.list[i].id;
                        uint32_t serial = cmd_in_vtseize->vts.list[i].serial;
                        printf("XUser %016lX is seizing VT %d, serial %010d\n", xuid, id, serial);
                        
                        uint16_t gi = cmd_vtseize.vts.len++;
                        cmd_vtseize.vts.list[gi].id = id;
                        cmd_vtseize.vts.list[gi].serial = serial;
                    }
                    
                    printf("VT count %d\n", cmd_vtseize.vts.len);
                    
                    fseek(pdata, 0, SEEK_SET);
                    fwrite(&cmd_vtseize.vts, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_vtseize(client);
                break;
            case 0x104A: // Exchange Token
                struct command_exchangeTokenReq * cmd_in_exchangetoken = cmd_in_raw;
                printf("Exchange token trade partner name: %.16s\n", cmd_in_exchangetoken->name);
                
                cmd_exchangetoken.token = 12345;
                send_command(client, &cmd_exchangetoken);
                break;
            case 0x104B: // Exchange User Update
                struct command_exchangeUserUpdateReq * cmd_in_exchangeuserupdate = cmd_in_raw;
                printf("Exchange user update trade partner name: %.16s, token: %d\n", cmd_in_exchangeuserupdate->name, cmd_in_exchangeuserupdate->token);
                
                snprintf(pdata_path, 256, "data/%016lX/pilot.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fseek(pdata, 1, SEEK_SET);
                    fwrite(cmd_in_exchangeuserupdate->data, sizeof(uint8_t), PILOT_DATA_SIZE, pdata);
                    fclose(pdata);
                }
                
                cmd_exchangeuserupdate.delay = 1;
                send_command(client, &cmd_exchangeuserupdate);
                break;
            case 0x1045: // Exchange VT
                struct command_exchangeVT * cmd_in_exchangevt = cmd_in_raw;
                struct vt_list * vt_rx = &cmd_in_exchangevt->vts;
                struct vt_list * vt_tx = get_exchange_vtlist(cmd_in_exchangevt);
                printf("Exchange VT trade partner name: %.16s, token %d\n", cmd_in_exchangevt->name, cmd_in_exchangevt->token);
                /*
                for (int i = 0; i < 60 && i < vt_rx->len; i++) {
                    printf( "VT List (Recived): ID %d, Serial %010d\n", vt_rx->list[i].id, vt_rx->list[i].serial);
                }
                if (!vt_rx->len) printf("VT List A is empty\n");
                                
                for (int i = 0; i < 60 && i < vt_tx->len; i++) {
                    printf( "VT List (Transfered): ID %d, Serial %010d\n", vt_tx->list[i].id, vt_tx->list[i].serial);
                }
                if (!vt_tx->len) printf("VT List B is empty\n");
                */
                cmd_exchangevt.vts.len = 0;
                snprintf(pdata_path, 256, "data/%016lX/garage.data", xuid);
                pdata = fopen(pdata_path, "rb+");
                if (pdata) {
                    fread(&cmd_exchangevt.vts, sizeof(struct vt_list), 1, pdata);

                    printf("VT count %d\n", cmd_exchangevt.vts.len);
                    // Remove VTs being transfered out
                    for (int i = 0; i < vt_tx->len; i++) {
                        uint16_t id = vt_tx->list[i].id;
                        uint32_t serial = vt_tx->list[i].serial;
                        printf("XUser %016lX is transfering VT %d, serial %010d\n", xuid, id, serial);
                        
                        for (int j = 0; j < cmd_exchangevt.vts.len; j++) {
                            // Find a VT with the same ID and Serial
                            if (cmd_exchangevt.vts.list[j].id == id && cmd_exchangevt.vts.list[j].serial == serial) {
                                // Remove the VT by overwriting it with the one at the end of the list
                                uint16_t gi = --cmd_exchangevt.vts.len;
                                cmd_exchangevt.vts.list[j].id = cmd_exchangevt.vts.list[gi].id;
                                cmd_exchangevt.vts.list[j].serial = cmd_exchangevt.vts.list[gi].serial;
                                
                                printf("VT Deposited\n");
                                break;
                            }
                        }
                    }

                    // Add VTs being recived
                    for (int i = 0; i < vt_rx->len; i++) {
                        uint16_t id = vt_rx->list[i].id;
                        uint32_t serial = vt_rx->list[i].serial;
                        printf("XUser %016lX is receiving VT %d, serial %010d\n", xuid, id, serial);
                        
                        uint16_t gi = cmd_exchangevt.vts.len++;
                        cmd_exchangevt.vts.list[gi].id = id;
                        cmd_exchangevt.vts.list[gi].serial = serial;
                    }
                    
                    printf("VT count %d\n", cmd_exchangevt.vts.len);
                    
                    fseek(pdata, 0, SEEK_SET);
                    fwrite(&cmd_exchangevt.vts, sizeof(struct vt_list), 1, pdata);
                    fclose(pdata);
                }
                
                send_command_vtget(client, &cmd_exchangevt);
                break;
            case 0x304B: // Exchange Confirm
                struct command_exchangeUserConfirm * cmd_in_exchangeuserconfirm = cmd_in_raw;
                printf("Exchange User Confirm trade partner name: %.16s, token: %d\n",
                    cmd_in_exchangeuserconfirm->name, cmd_in_exchangeuserconfirm->token);

                send_command(client, &cmd_exchangeuserconfirm);
                break;
            case 0x1028: // Request Supply Points (Used at end of turn)
                struct command_arg2 * cmd_in_supplypoints = cmd_in_raw;
                printf("Request supply points: Last round was %d\n", cmd_in_supplypoints->arg);
                
                cmd_supplypoint.points = 10000;
                send_command(client, &cmd_supplypoint);
                break;
            case 0x1050: // Get number of messages in board
                struct command_arg1a * cmd_in_messboardnum = cmd_in_raw;
                printf("Message Board Number args: %d %d\n", cmd_in_messboardnum->arg[0], cmd_in_messboardnum->arg[1]);
                
                // Send back an error to disable this feature
                send_command(client, &cmd_boardmessnum);
                break;
            case 0x1052: // Post new message to board
                // Send back an error to disable this feature
                send_command(client, &cmd_boardmessregist);
                break;
            case 0x3030:
                send_command(client, &cmd_ptunk);
                break;
            default:
                printf("Unknown command: %04X\n", cmd_in_base->code);
            }
        }
        close(client);

        printf("Client %s disconnected\n", client_addr_str);
    }
    
    return 0;
}
