use axum::{
    body::Body,
    extract::Query,
    http::{header, HeaderValue},
    response::Response,
    RequestPartsExt,
};

use crate::handler::{
    common::{get_raw_object, object_headers, s3_error::S3Error},
    get::{override_headers, GetObjectHeaderOpts, GetObjectQueryOpts},
    ObjectRequestContext,
};

pub async fn head_object_handler(ctx: ObjectRequestContext) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let mut parts = ctx.request.into_parts().0;
    let Query(query_opts): Query<GetObjectQueryOpts> = parts.extract().await?;
    let header_opts = GetObjectHeaderOpts::from_headers(&parts.headers)?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;
    let obj = get_raw_object(&ctx.app, &bucket.root_blob_name, &ctx.key).await?;

    let mut resp = Response::new(Body::empty());
    resp.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&obj.size()?.to_string())?,
    );
    object_headers(&mut resp, &obj, checksum_mode_enabled)?;
    override_headers(&mut resp, &query_opts)?;
    Ok(resp)
}
