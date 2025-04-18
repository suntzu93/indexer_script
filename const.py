HOST = "https://api.graphindexer.suntzu.dev"
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
CELO_RPC_ENDPOINT = "https://celo.drpc.org"
POLYGON_RPC_ENDPOINT = "https://polygon.drpc.org"
FANTOM_RPC_ENDPOINT = "https://rpc2.fantom.network"
ETH_RPC_ENDPOINT = "https://eth.drpc.org"
BSC_RPC_ENDPOINT = "https://bsc.drpc.org"
POA_RPC_ENDPOINT = "https://core.poa.network"
AVAX_RPC_ENDPOINT = "https://avalanche.drpc.org"
ARBITRUM_RPC_ENDPOINT = "https://arbitrum.drpc.org"
ARBITRUM_NOVA_RPC_ENDPOINT = "https://nova.arbitrum.io/rpc	"
GNOSIS_RPC_ENDPOINT = "https://gnosis.drpc.org"
GOERLI_RPC_ENDPOINT = "https://rpc.ankr.com/eth_goerli"
OPTIMISM_RPC_ENDPOINT = "https://optimism.drpc.org"
FUSE_RPC_ENDPOINT = "https://fuse.drpc.org"
AURORA_RPC_ENDPOINT = "https://mainnet.aurora.dev"
HARMONY_RPC_ENDPOINT = "https://rpc.ankr.com/harmony"
BASE_RPC_ENDPOINT = "https://base.drpc.org"
SCROLL_RPC_ENDPOINT = "https://scroll.drpc.org"
LINEA_RPC_ENDPOINT = "https://linea.drpc.org"
BLAST_RPC_ENDPOINT = "https://blast.drpc.org"
MOONBEAM_RPC_ENDPOINT = "https://moonbeam.drpc.org"
SONIC_RPC_ENDPOINT= "https://sonic.drpc.org"
POLYGON_ZK_RPC_ENDPOINT = "https://polygon-zkevm.drpc.org"
SONEIUM_RPC_ENDPOINT = "https://rpc.soneium.org"
BASE_SEPOLIA_RPC_ENDPOINT = "https://sepolia.base.org"
SEPOLIA_RPC_ENDPOINT = "https://eth-sepolia.public.blastapi.io"

VERSION = "1.0.2"