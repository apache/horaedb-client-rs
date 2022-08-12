use ceresdbproto::storage::{
    Route as RoutePb, RouteRequest as RouteRequestPb, RouteResponse as RouteResponsePb,
};

#[derive(Debug, Clone)]
pub struct RouteRequest {
    pub metrics: Vec<String>,
}

impl From<RouteRequest> for RouteRequestPb {
    fn from(req: RouteRequest) -> Self {
        let mut req_pb = RouteRequestPb::default();
        req_pb.set_metrics(req.metrics.into());

        req_pb
    }
}

#[derive(Debug, Clone)]
pub struct RouteResponse {
    pub routes: Vec<Route>,
}

/// Route info about metric, contains: metric and endpoint.
///
/// Endpoint is possible to be none if metric not found in cluster.
#[derive(Debug, Default, Clone, Hash)]
pub struct Route {
    pub metric: String,
    pub endpoint: Option<EndPoint>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct EndPoint {
    pub ip: String,
    pub port: u32,
}

impl EndPoint {
    pub fn new(ip: String, port: u32) -> Self {
        Self { ip, port }
    }
}

impl From<RouteResponsePb> for RouteResponse {
    fn from(req_pb: RouteResponsePb) -> Self {
        let end_points: Vec<Route> = req_pb.routes.into_iter().map(|r| r.into()).collect();
        Self { routes: end_points }
    }
}

impl From<RoutePb> for Route {
    fn from(route_pb: RoutePb) -> Self {
        let endpoint = if route_pb.has_endpoint() {
            let endpoint_pb = route_pb.endpoint.unwrap();
            Some(EndPoint {
                ip: endpoint_pb.ip,
                port: endpoint_pb.port,
            })
        } else {
            None
        };

        Self {
            metric: route_pb.metric,
            endpoint,
        }
    }
}
