// ./client <IP>:<PORT> tracker_info.txt
//
// <IP>:<PORT> is this client's own address and PORT for seeding
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "../common/net.h"
#include "../common/buffer.h"
#include "../common/sha1.h"
#include "tracker_client.h"
#include "seeder.h"
#include "download_manager.h"

using namespace p2p;

static std::vector<std::string> tokenize(const std::string& line) {
    std::vector<std::string> t;
    std::istringstream is(line);
    std::string w;
    while (is >> w) t.push_back(w);
    return t;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <IP>:<PORT> <tracker_info.txt>\n", argv[0]);
        return 1;
    }
    // parse arguments
    std::string self_ip;
    uint16_t self_port = 0;
    if (!parse_endpoint(argv[1], self_ip, self_port)) {
        std::fprintf(stderr, "bad address: %s\n", argv[1]);
        return 1;
    }
    // list of all trackers
    std::vector<TrackerEndpoint> eps;
    {
        std::ifstream f(argv[2]);
        std::string ip; int port;
        while (f >> ip >> port) eps.push_back({ip, static_cast<uint16_t>(port)});
    }
    if (eps.empty()) { std::fprintf(stderr, "no trackers in %s\n", argv[2]); return 1; }
    // check for a tracker to connect to
    TrackerClient tracker;
    tracker.configure(eps);
    if (!tracker.connect_any()) {
        std::fprintf(stderr, "no tracker reachable\n");
        return 1;
    }

    Seeder seeder;
    if (!seeder.start(self_ip, self_port)) {
        std::fprintf(stderr, "could not bind seeder port %u\n", self_port);
        return 1;
    }
    DownloadManager downloads(seeder);

    std::string line;
    std::string logged_in_as;
    while (std::getline(std::cin, line)) {
        auto t = tokenize(line);
        if (t.empty()) continue;

        uint16_t status = ST_ERR;
        std::string resp;

        // commands
        // create_user <user id> <password> - Register a new user account *
        // login <user id> <password> - Authenticate and start a session *
        // create_group <group id> - Create a new group (user becomes owner)
        // join_group <group id> - Request to join an existing group
        // leave_group <group id> - Leave a group you’re a member of
        // list_groups - Display all available groups in the system *
        // list_requests <group id> - Show pending join requests (owner only)
        // accept_request <group id> <user id> - Accept a join request (owner only)
        // logout - End current session and stop sharing files *
        // upload_file <group id> <file path> - Share a file with a group *
        // list_files <group id> - Show all files available in a group
        // download_file <group id> <file name> <destination path> - Download a file from the group *
        // show_downloads - Display current download progress *
        // stop share <group id> <file name> - Stop sharing a specific file

        // create_user <user_id> <pwd>
        if (t.size() == 3 && t[0] == "create_user") {
            Buffer b; 
            b.put_str(t[1]); // uid 
            b.put_str(t[2]); // pwd
            if (tracker.request(MSG_CREATE_USER, b.str(), status, resp))
                std::cout << status_str(status) << "\n";
            else std::cout << "tracker unreachable\n";
        } 
        // login <user_id> <pwd>
        else if (t.size() == 3 && t[0] == "login") {
            Buffer b; b.put_str(t[1]); b.put_str(t[2]); b.put_u16(self_port);
            if (tracker.request(MSG_LOGIN, b.str(), status, resp)) {
                if (status == ST_OK) logged_in_as = t[1];
                std::cout << status_str(status) << "\n";
            } else std::cout << "tracker unreachable\n";
        }
        // list *only*
        else if (t.size() == 1 && t[0] == "list" ) {
            std::cout << "usage: list groups | list files <group> | list requests <group>\n";

        } 
        // list_groups
        else if (t.size() == 1 && t[0] == "list_groups") {
            if (tracker.request(MSG_LIST_GROUPS, "", status, resp) && status == ST_OK) {
                Buffer in(resp);
                uint32_t n = 0; in.get_u32(n);
                for (uint32_t i = 0; i < n; ++i) { std::string g; in.get_str(g); std::cout << g << "\n"; }
            } else std::cout << status_str(status) << "\n";

        } 
        // create_group <group id>
        else if (t.size() == 2 && t[0] == "create_group") {
            Buffer b; b.put_str(t[1]);
            if (tracker.request(MSG_CREATE_GROUP, b.str(), status, resp))
                std::cout << status_str(status) << "\n";
            else std::cout << "tracker unreachable\n";


        }
        // join_group <group id>
        else if (t.size() == 2 && t[0] == "join_group") {
            Buffer b; b.put_str(t[1]);
            if (tracker.request(MSG_JOIN_GROUP, b.str(), status, resp))
                std::cout << status_str(status) << "\n";
            else std::cout << "tracker unreachable\n";
        }
        // leave_group <group id>
        else if (t.size() == 2 && t[0] == "leave_group") {
            Buffer b; b.put_str(t[1]);
            if (tracker.request(MSG_LEAVE_GROUP, b.str(), status, resp))
                std::cout << status_str(status) << "\n";
            else std::cout << "tracker unreachable\n";
        }
        // list_requests <group id>
        else if (t.size() == 2 && t[0] == "list_requests") {
            Buffer b; b.put_str(t[1]);
            if (tracker.request(MSG_LIST_REQUESTS, b.str(), status, resp) && status == ST_OK) {
                Buffer in(resp);
                uint32_t n = 0; in.get_u32(n);
                for (uint32_t i = 0; i < n; i++) { std::string u; in.get_str(u); std::cout << u << "\n"; }
            } else std::cout << status_str(status) << "\n";

        }
        // accept_request <group id> <user id>
        else if (t.size() == 3 && t[0] == "accept_request") {
            Buffer b; b.put_str(t[1]); b.put_str(t[2]);
            if (tracker.request(MSG_ACCEPT_REQUEST, b.str(), status, resp))
                std::cout << status_str(status) << "\n";
            else std::cout << "tracker unreachable\n";
        }
        // upload_file <group id> <file path>
        else if (t.size() == 3 && t[0] == "upload_file") {
            // Hash locally, publish metadata, then start seeding.
            std::string piece_blob;
            uint64_t size = 0;
            std::string whole = SHA1::hash_file(t[3], PIECE_SIZE, &piece_blob, &size);
            if (whole.empty()) { std::cout << "cannot read file\n"; continue; }
            // TODO: pack {group, filename, size, whole, piece hashes} into a
            // Buffer, send MSG_UPLOAD_FILE, and on ST_OK create a PieceStore
            // with open_for_seed() and hand it to seeder.add_share().
            std::cout << "TODO: upload (" << size << " bytes, "
                      << piece_blob.size() / SHA1_HEX_LEN << " pieces, sha1 "
                      << whole << ")\n";
        } 
        // download_file <group id> <file name> <destination path>
        else if (t.size() == 4 && t[0] == "download_file") {
            // TODO: MSG_GET_FILE_META -> build a DownloadJob -> downloads.start()
            std::cout << "TODO: download\n";

        } 
        // show_downloads
        else if (t.size() == 2 && t[0] == "show_downloads") {
            for (const auto& s : downloads.status_lines()) std::cout << s << "\n";

        } 
        // logout
        else if (t.size() == 1 && t[0] == "logout") {
            Buffer b; b.put_str(logged_in_as);
            tracker.request(MSG_LOGOUT, b.str(), status, resp);
            logged_in_as.clear();
            std::cout << "logged out\n";

        } 
        // exit & quit
        else if (t.size() == 1 && (t[0] == "exit" || t[0] == "quit")) {
            break;

        } else {
            // TODO: list files / stop share
            std::cout << "unrecognised or not yet implemented: " << line << "\n";
        }
    }

    seeder.stop();
    tracker.disconnect();
    return 0;
}
