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

#include "../common/net.hpp"
#include "../common/buffer.hpp"
#include "../common/sha1.hpp"
#include "tracker_client.hpp"
#include "seeder.hpp"
#include "download_manager.hpp"

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
    DownloadManager downloads(seeder, tracker);

    std::string line;
    std::string logged_in_as;
    while (1) {
        std::printf("[%s:%u] Client >", self_ip.c_str(), self_port);

        if(!std::getline(std::cin, line)) break;
        
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
            if (logged_in_as.empty()) { std::cout << "not logged in\n"; continue; }
            const std::string& path = t[2];
            // Hash locally, publish metadata, then start seeding.
            std::string piece_blob;
            uint64_t size = 0;
            std::string whole = SHA1::hash_file(path, PIECE_SIZE, &piece_blob, &size);
            if (whole.empty()) { std::cout << "cannot read file\n"; continue; }

            uint32_t piece_count = static_cast<uint32_t>(piece_blob.size() / SHA1_HEX_LEN);
            std::string fname = path.substr(path.find_last_of('/') + 1); // basename

            Buffer b;
            b.put_str(t[1]);          // group
            b.put_str(fname);         // published name
            b.put_u64(size);
            b.put_str(whole);
            b.put_u32(piece_count);
            for (uint32_t i = 0; i < piece_count; i++)
                b.put_str(piece_blob.substr(i * SHA1_HEX_LEN, SHA1_HEX_LEN));

            if (!tracker.request(MSG_UPLOAD_FILE, b.str(), status, resp)) {
                std::cout << "tracker unreachable\n";
                continue;
            }
            if (status != ST_OK) { std::cout << status_str(status) << "\n"; continue; }

            auto store = std::make_shared<PieceStore>();
            if (!store->open_for_seed(path, size, piece_count)) {
                std::cout << "uploaded metadata, but could not reopen file for seeding\n";
                continue;
            }
            seeder.add_share(ShareKey{t[1], fname}, store);
            std::cout << "uploaded " << fname << " (" << size << " bytes, "
                      << piece_count << " pieces)\n";
        }
        // list_files <group id>
        else if (t.size() == 2 && t[0] == "list_files") {
            Buffer b; b.put_str(t[1]);
            if (tracker.request(MSG_LIST_FILES, b.str(), status, resp) && status == ST_OK) {
                Buffer in(resp);
                uint32_t n = 0; in.get_u32(n);
                for (uint32_t i = 0; i < n; ++i) { std::string f; in.get_str(f); std::cout << f << "\n"; }
            } else std::cout << status_str(status) << "\n";
        }
        // download_file <group id> <file name> <destination path>
        else if (t.size() == 4 && t[0] == "download_file") {
            if (logged_in_as.empty()) { std::cout << "not logged in\n"; continue; }
            Buffer b; b.put_str(t[1]); b.put_str(t[2]);
            if (!tracker.request(MSG_GET_FILE_META, b.str(), status, resp)) {
                std::cout << "tracker unreachable\n";
                continue;
            }
            if (status != ST_OK) { std::cout << status_str(status) << "\n"; continue; }

            Buffer in(resp);
            auto job = std::make_shared<DownloadJob>();
            job->group = t[1];
            job->file = t[2];
            job->dest_path = t[3];
            job->user_id = logged_in_as;
            uint32_t pc = 0;
            if (!in.get_u64(job->size) || !in.get_str(job->file_hash) || !in.get_u32(pc)) {
                std::cout << "malformed metadata from tracker\n";
                continue;
            }
            job->piece_hashes.resize(pc);
            for (uint32_t i = 0; i < pc; i++) in.get_str(job->piece_hashes[i]);
            uint32_t npeers = 0;
            in.get_u32(npeers);
            for (uint32_t i = 0; i < npeers; i++) {
                PeerAddr pa;
                uint32_t nbits = 0;
                in.get_str(pa.user_id); in.get_str(pa.ip); in.get_u16(pa.port);
                in.get_u32(nbits);
                std::vector<uint8_t> bits(nbits);
                if (nbits > 0) in.get_raw(bits.data(), nbits);
                if (pa.user_id != logged_in_as) job->peers.push_back(pa);
            }
            if (job->peers.empty()) { std::cout << "no online peers hold this file\n"; continue; }

            if (downloads.start(job)) std::cout << "download started\n";
            else std::cout << "a download for this group/file is already running\n";
        }
        // show_downloads
        else if (t.size() == 1 && t[0] == "show_downloads") {
            for (const auto& s : downloads.status_lines()) std::cout << s << "\n";

        }
        // stop_share <group id> <file name>
        else if (t.size() == 3 && t[0] == "stop_share") {
            Buffer b; b.put_str(t[1]); b.put_str(t[2]);
            if (!tracker.request(MSG_STOP_SHARE, b.str(), status, resp)) {
                std::cout << "tracker unreachable\n";
                continue;
            }
            if (status == ST_OK) seeder.remove_share(ShareKey{t[1], t[2]});
            std::cout << status_str(status) << "\n";
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
            std::cout << "unrecognised command: " << line << "\n";
        }
    }

    seeder.stop();
    tracker.disconnect();
    return 0;
}
