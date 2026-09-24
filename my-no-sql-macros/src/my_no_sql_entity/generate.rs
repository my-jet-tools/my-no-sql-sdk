extern crate proc_macro;

use proc_macro::TokenStream;
use syn::DeriveInput;
use types_reader::{StructProperty, TokensObject};

use crate::MyNoSqlEntityParameters;

pub fn generate(
    attr: proc_macro2::TokenStream,
    input: proc_macro::TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input_token_stream: proc_macro2::TokenStream = input.clone().into();

    let derive = crate::entity_utils::extract_attributes(&input_token_stream);

    let input: DeriveInput = syn::parse(input).unwrap();

    let struct_name = &input.ident;

    let fields = StructProperty::read(&input)?;

    let attr: TokensObject = attr.try_into()?;

    let params = MyNoSqlEntityParameters::try_from(&attr)?;

    let result = super::generate_base_impl(
        struct_name,
        derive,
        fields.as_slice(),
        params.table_name,
        params.with_expires.unwrap_or(false),
    )?;

    Ok(result.into())
}
