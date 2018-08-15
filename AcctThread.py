#!/usr/bin/env python3

#    Copyright (c) 2018 Ripple Labs Inc.
#
#    Permission to use, copy, modify, and/or distribute this software for any
#    purpose  with  or without fee is hereby granted, provided that the above
#    copyright notice and this permission notice appear in all copies.
#
#    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
#    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
#    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
#    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
#    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
#    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
#    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# Walk back through an account node's history and report all transactions
# that affect the account in reverse order (from newest to oldest).
#
# To capture results redirect stdout to a file.

import json
import re
import sys
import websocket

from websocket import create_connection


def extract_args():
    '''Extract command line arguments'''

    # Default websocket connection if none provided.
    connect_to = "ws://s2.ripple.com:443"

    usage = f'''usage: PyAcctThread <Account ID> [ws://<Server>:<port>]
If <server>:<port> are omitted defaults to "{connect_to}"'''

    # Extract the first argument: the accountID
    arg_count = len(sys.argv) - 1
    if arg_count < 1:
        print('Expected account ID as first argument.\n')
        print(usage)
        sys.exit(1)  # abort because of error

    # Sanity check the accountID string
    account_id = sys.argv[1]
    id_len = len(account_id)
    if (account_id[:1] != "r") or (id_len < 25) or (id_len > 35):
        print(
            'Invalid format for account ID.\n',
            'Should start with "r" with length between 25 and 35 characters.\n'
        )
        print(usage)
        sys.exit(1)  # abort because of error

    if arg_count > 2:
        print('Too many command line arguments.\n')
        print(usage)
        sys.exit(1)  # abort because of error

    # If it's there, pick up the optional connect_to.
    if arg_count == 2:
        connect_to = sys.argv[2]

    # Validate the connect_to.
    if connect_to[:5] != "ws://":
        print('Invalid format for websocket connection.  Expected "ws://".  ',
              f'Got: {connect_to}\n')
        print(usage)
        sys.exit(1)  # abort because of error

    # Verify that the port is specified.
    if not re.search(r'\:\d+$', connect_to):
        print('Invalid format for websocket connection.  Connection expected ',
              'to end with \nport specifier (colon followed by digits), ',
              'e.g., ws://s2.ripple.com:443')
        print(f'Got: {connect_to}\n')
        print(usage)
        sys.exit(1)  # abort because of error

    return connect_to, account_id


def ws_id():
    '''Generate a new websocket request ID and return it'''
    ws_id.value += 1
    return ws_id.value
ws_id.value = 0


def get_response(ws, req_id):
    '''Get the websocket response that matches the id of the request.
       All responses are in json, so return json.'''
    while True:
        msg = ws.recv()
        json_msg = json.loads(msg)
        got_id = json_msg["id"]
        if got_id == req_id:
            return json_msg
        print(
            f"Unexpected websocket message id: {got_id}.  Expected {req_id}.")


def print_account_info(ws, account_id):
    '''Request account_info, print it, and return it as JSON'''
    wsid = ws_id()

    cmd = f"""
{{
    "id" : {wsid},
    "command" : "account_info",
    "account" : "{account_id}",
    "strict" : true,
    "ledger_index" : "validated"
}}"""

    ws.send(cmd)
    json_msg = get_response(ws, wsid)

    # Remove the websocket id to reduce noise.
    json_msg.pop("id", None)
    print(json.dumps(json_msg, indent=4))
    return json_msg


def print_tx(ws, txn_id):
    '''Request tx by tnx_id, print it, and return the tx as JSON'''
    wsid = ws_id()

    cmd = f"""
{{
    "id" : {wsid},
    "command" : "tx",
    "transaction" : "{txn_id}"
}}"""

    ws.send(cmd)
    json_msg = get_response(ws, wsid)

    # Remove the websocket id from what we print to reduce noise.
    json_msg.pop("id", None)
    print(json.dumps(json_msg, indent=4))
    return json_msg


def get_prev_txn_id(json_msg, account_id):
    '''Extract the PreviousTxnID for accountID given a transaction's metadata'''

    # Handle any errors that might be in the response.
    if "error" in json_msg:

        # If the error is txnNotFound there there's a good chance
        # the server does not have enough history.
        if json_msg["error"] == "txnNotFound":
            print("Transaction not found.",
                  "Does your server have enough history? Unexpected stop.")
            return ""

        err = json_msg["error"]
        if json_msg['error_message']:
            err = json_msg['error_message']

        print(f"{err}  Unexpected stop.")
        return ""

    # First navigate to the metadata AffectedNodes, which should be a list.
    try:
        affected_nodes = json_msg["result"]["meta"]["AffectedNodes"]
    except KeyError:
        print("No AffectedNodes found in transaction.  Unexpected stop.\n")
        print("Got:")
        print(json.dumps(json_msg, indent=4))
        return ""

    for node in affected_nodes:
        # If we find the account being created then we're successfully done.
        try:
            if node["CreatedNode"]["LedgerEntryType"] == "AccountRoot":
                if node["CreatedNode"]["NewFields"]["Account"] == account_id:
                    print(f'Created Account {account_id}.  Done.')
                    return ""
        except KeyError:
            pass  # If the field is not found that's okay.

        # Else look for the next transaction.
        try:
            if node["ModifiedNode"]["FinalFields"]["Account"] == account_id:
                return node["ModifiedNode"]["PreviousTxnID"]
        except KeyError:
            continue
            # If the field is not found try the next node.

    print("No more modifying transactions found.  Unexpected stop.\n")
    print("Got:")
    print(json.dumps(json_msg, indent=4))
    return ""


def spinning_cursor():
    '''Write spinner to stderr to show liveness while walking the thread'''
    while True:
        for cursor in '|/-\\':
            sys.stderr.write(cursor)
            sys.stderr.flush()
            yield
            sys.stderr.write('\b')


def thread_account(ws, account_id):
    '''Thread the account to extract all transactions performed by that account.
       1. Start by getting account_info for the account_id in the most recent
          validated ledger.
       2. account_info returns the TxId of the last transaction that affected
          this account.
       3. Call tx with that TxId.  Save that transaction.
       4. The tx response should contain the AccountRoot for the account_id.
          Extract the new value of PreviousTxID from the AccountRoot.
       5. Return to step 3, but using the new PreviousTxID.'''

    # Call account_info to get our starting txId.
    json_msg = print_account_info(ws, account_id)

    # Handle any errors that might be in the response.
    if "error" in json_msg:
        print(f'No account_info for accountID {account_id}')
        if json_msg["error"] == "actMalformed":
            print("Did you mistype the accountID?")

        err = json_msg["error"]
        if json_msg["error_message"]:
            err = json_msg["error_message"]

        print(f"{err}  Unexpected stop.")
        return

    # Extract the starting txId.
    prev_txn_id = ""
    try:
        prev_txn_id = json_msg["result"]["account_data"]["PreviousTxnID"]
    except KeyError:
        print(
            f"No PreviousTxnID found for {account_id}.  No transactions found."
        )
        return

    # Transaction threading loop.
    spinner = spinning_cursor()  # Liveness indicator.
    while prev_txn_id != "":
        next(spinner)
        print("\n" + ("-" * 79) + "\n")
        json_msg = print_tx(ws, prev_txn_id)
        prev_txn_id = get_prev_txn_id(json_msg, account_id)


def open_connection(connect_to, account_id):
    '''Open the websocket connection and pass the websocket upstream for use'''
    try:
        ws = create_connection(connect_to)
    except websocket._exceptions.WebSocketAddressException:
        print(f"Unable to open connection to {connect_to}.\n")
        return
    except ConnectionRefusedError:
        print(f"Connection to {connect_to} refused.\n")
        return

    try:
        thread_account(ws, account_id)
    finally:
        ws.close()
        sys.stderr.write('\b')  # Clean up from spinning_cursor


if __name__ == "__main__":
    # Open the connection then thread the account.
    open_connection(*extract_args())
