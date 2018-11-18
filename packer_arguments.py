def get_packer_arguments(action_name, **action_input):
    action_arguments = {"CREATE": {"where":
                                       {"dir": action_input.get("dir"),
                                        "Name": action_input.get("name")},
                                   "Create Mode": action_input.get("create_mode"),
                                   "obj_attributes":
                                       {"mode": 0,
                                        "uid": 0,
                                        "gid": 0,
                                        "size": 0,
                                        "atime": (0, 0),
                                        "mtime": (0, 0)},
                                   },

                        "LOOKUP": {"what":
                                       {"dir": action_input.get("dir"),
                                        "Name": action_input.get("name")}},

                        "WRITE": {"file": action_input.get("file"),
                                  "offset": action_input.get("offset"),
                                  "count": action_input.get("count"),
                                  "Stable": action_input.get("stable"),
                                  "Data": action_input.get("data")},

                        "READDIR": {"dir": action_input.get("dir"),
                                    "cookie": 0,
                                    "Verifier": 0,
                                    "count": 2000},

                        "READDIRPLUS": {"dir": action_input.get("dir"),
                                        "cookie": 0,
                                        "Verifier": 0,
                                        "count": 2000,
                                        "maxcount": 2000,
                                        },

                        "LOCK": {"cookie":  (4, ''),
                                 "block": action_input.get("block"),
                                 "exclusive": action_input.get("exclusive"),
                                 "lock":
                                     {"caller_name": action_input.get("caller_name"),
                                      "fh": action_input.get("fh"),
                                      "owner": action_input.get("owner"),
                                      "svid": 4,
                                      "l_offset": action_input.get("l_offset"),
                                      "l_len": action_input.get("l_len")},
                                 "reclaim": False,
                                 "state": 3},

                        "UNLOCK": {"cookie": (4, ''),
                                   "lock":
                                       {"caller_name": action_input.get("caller_name"),
                                        "fh": action_input.get("fh"),
                                        "owner": action_input.get("owner"),
                                        "svid": 4,
                                        "l_offset": action_input.get("l_offset"),
                                        "l_len": action_input.get("l_len")}
                                   }
                        }

    return action_arguments[action_name]
