use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    Ident, LitStr, Path, Result, Token,
};

struct RegisterStrategyInput {
    ty: Path,
    canonical: LitStr,
    aliases: Vec<LitStr>,
}

impl Parse for RegisterStrategyInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let ty: Path = input.parse()?;
        input.parse::<Token![,]>()?;
        let canonical: LitStr = input.parse()?;
        let mut aliases = Vec::new();

        if input.peek(Token![,]) {
            let fork = input.fork();
            fork.parse::<Token![,]>()?;
            if fork.peek(Ident) {
                input.parse::<Token![,]>()?;
                let ident: Ident = input.parse()?;
                if ident != "aliases" {
                    return Err(syn::Error::new(
                        ident.span(),
                        "expected `aliases = [\"foo\", \"bar\"]`",
                    ));
                }
                input.parse::<Token![=]>()?;
                let content;
                syn::bracketed!(content in input);
                let parsed_aliases: Punctuated<LitStr, Token![,]> =
                    content.parse_terminated(|input| input.parse(), Token![,])?;
                aliases = parsed_aliases.into_iter().collect();
            }
        }

        if input.peek(Token![,]) {
            let _ = input.parse::<Token![,]>();
        }

        if !input.is_empty() {
            return Err(input.error("unexpected tokens after register_strategy! arguments"));
        }

        Ok(Self {
            ty,
            canonical,
            aliases,
        })
    }
}

#[proc_macro]
pub fn register_strategy(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as RegisterStrategyInput);
    let ty = args.ty;
    let canonical = args.canonical;
    let alias_literals = args.aliases;
    let type_ident = ty
        .segments
        .last()
        .expect("strategy type must contain at least one segment")
        .ident
        .clone();
    let factory_ident = format_ident!("__{}_StrategyFactory", type_ident);
    let register_ident = format_ident!(
        "__register_{}_strategy",
        type_ident.to_string().to_lowercase()
    );

    let expanded = quote! {
        const _: () = {
            #[allow(non_camel_case_types)]
            struct #factory_ident;

            impl ::tesser_strategy::StrategyFactory for #factory_ident {
                fn canonical_name(&self) -> &'static str {
                    #canonical
                }

                fn aliases(&self) -> &'static [&'static str] {
                    &[ #( #alias_literals ),* ]
                }

                fn build(&self, params: ::toml::Value) -> ::tesser_strategy::StrategyResult<Box<dyn ::tesser_strategy::Strategy>> {
                    let mut strategy = Box::new(<#ty as ::core::default::Default>::default());
                    strategy.configure(params)?;
                    Ok(strategy)
                }
            }

            #[ctor::ctor]
            fn #register_ident() {
                ::tesser_strategy::register_strategy_factory(::std::sync::Arc::new(#factory_ident));
            }
        };
    };

    TokenStream::from(expanded)
}
