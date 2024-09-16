HOST = "https://api.graphindexer.co/"
API_GET_CLOSE_ACTIONS = HOST + "/api/closeActions"
API_GET_OPEN_ACTIONS = HOST + "/api/openActions"
API_UPDATE_EXE_STATUS = HOST + "/api/updateExeStatus"
API_UPDATE_TIME = HOST + "/api/updateTime"
API_ALERT_RPC = HOST + "/api/alertRpc"

CLOSE_ACTION = 1
OPEN_ACTION = 2
OFFCHAIN_ACTION = 3
RE_ALLOCATION_ACTION = 4

STATUS_QUEUE = "Queue"
STATUS_APPROVE = "Approved"

GRAPHMAN_REASSIGN = "reassign"
GRAPHMAN_UNASSIGN = "unassign"
GRAPHMAN_REMOVE = "remove"
GRAPHMAN_REWIND = "rewind"
GRAPHMAN_PAUSE = "pause"
GRAPHMAN_RESUME = "resume"
GRAPHMAN_STATS_SHOW = "stats show"
GRAPHMAN_INFO = "info"
GRAPHMAN_COPY = "copy"
GRAPHMAN_DATABASE = "database"
GRAPHMAN_STATS_ACCOUNT_LIKE = "stats account-like"  # Added this new line
GRAPHMAN_SUBGRAPH_CREATE_DEPLOY = "subgraph_create_deploy"
GRAPHMAN_UNUSED = "unused"

# Error

TOKEN_ERROR = "TOKEN ERROR"
ERROR = "ERROR"
SUCCESS = "OK"

NEAR_RPC_ENDPOINT = "https://rpc.mainnet.near.org/status"
COSMOS_RPC_ENDPOINT = "https://cosmos-rpc.polkachu.com/status"
CELO_RPC_ENDPOINT = "https://rpc.ankr.com/celo"
POLYGON_RPC_ENDPOINT = "https://rpc.ankr.com/polygon"
FANTOM_RPC_ENDPOINT = "https://rpc.ankr.com/fantom"
ETH_RPC_ENDPOINT = "https://rpc.ankr.com/eth"
BSC_RPC_ENDPOINT = "https://rpc.ankr.com/bsc"
POA_RPC_ENDPOINT = "https://core.poa.network"
AVAX_RPC_ENDPOINT = "https://rpc.ankr.com/avalanche"
ARBITRUM_RPC_ENDPOINT = "https://rpc.ankr.com/arbitrum"
ARBITRUM_NOVA_RPC_ENDPOINT = "https://nova.arbitrum.io/rpc	"
GNOSIS_RPC_ENDPOINT = "https://rpc.ankr.com/gnosis"
GOERLI_RPC_ENDPOINT = "https://rpc.ankr.com/eth_goerli"
OPTIMISM_RPC_ENDPOINT = "https://mainnet.optimism.io"
FUSE_RPC_ENDPOINT = "https://fuse.drpc.org"
AURORA_RPC_ENDPOINT = "https://mainnet.aurora.dev"
HARMONY_RPC_ENDPOINT = "https://rpc.ankr.com/harmony"
BASE_RPC_ENDPOINT = "https://rpc.ankr.com/base"
SCROLL_RPC_ENDPOINT = "https://rpc.ankr.com/scroll"
LINEA_RPC_ENDPOINT = "https://rpc.linea.build"
BLAST_RPC_ENDPOINT = "https://rpc.ankr.com/blast"
MOONBEAM_RPC_ENDPOINT = "https://rpc.ankr.com/moonbeam"

VERSION = "1.0.2"