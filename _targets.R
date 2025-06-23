targets::tar_option_set(
  format = "qs",
  packages = c("config", "tigris", "readr")
)

# Targets Pipeline ====
list(
  ## Configure/Validate Parameters ====
  targets::tar_target(
    name = config_file,
    command = "config.yaml",
    format = "file"
  ),
  targets::tar_target(
    name = config,
    command = config::get(file = config_file) |>
      utils_validate_config()
  ),
  ## Local File Targets ====
  targets::tar_target(
    name = officers_file,
    command = config$officers_path,
    format = "file"
  ),
  targets::tar_target(
    name = companies_file,
    command = config$companies_path,
    format = "file"
  ),
  targets::tar_target(
    name = alt_names_file,
    command = config$alt_names_path,
    format = "file"
  ),
  targets::tar_target(
    name = parcels_meta,
    command = {
      response <- httr::HEAD(config$parcels_remote)
      httr::headers(response)[["last-modified"]]
    }
  ),
  ## Remote GDB Target ====
  targets::tar_target(
    name = parcels_zip,
    {
      dummy <- parcels_meta
      path <- base::file.path(config$data_dir, "parcels.zip")
      load_remote(
        url = config$parcels_remote, 
        path = path
      )
      path
    },
    format = "file"
  ),
  targets::tar_target(
    parcels_path,
    command = {
      dir <- base::tempfile()
      utils::unzip(parcels_zip, exdir = dir)
      fs::dir_ls(dir, recurse = TRUE, regexp = "\\.gdb$", type = "directory")
    },
    format = "file"
  ),
  ## Load... ====
  ### ...Assessor's table ====
  targets::tar_target(
    assess_init,
    command = load_parcels(
      parcels_path,
      layer = config$assess_layer,
      layer_type = "assess",
      state = config$state,
      muni_ids = config$muni_ids
    )
  ),
  ### ...Parcels ====
  targets::tar_target(
    parcels_init,
    command = load_parcels(
      parcels_path,
      layer = config$parcels_layer,
      layer_type = "parcels",
      state = config$state,
      muni_ids = config$muni_ids
    )
  ),
  ### ...Boston Addresses ====
  targets::tar_target(
    name = address_bos,
    command = load_boston_addresses()
  ),
  ### ...Other MA Addresses ====
  targets::tar_target(
    name = address_massgis,
    command = load_massgis_addresses()
  ),
  ### ...Geographic Placenames ====
  targets::tar_target(
    name = geonames_init,
    command = load_geonames(config$state)
  ),
  ### ...Block Groups (`tigris`) ====
  targets::tar_target(
    name = cbgs, 
    command = tigris::block_groups(config$state) |>
      st_preprocess(config$crs) |>
      dplyr::select(id = geoid)
  ),
  ### ...ZIPs (`tigris`) ====
  targets::tar_target(
    name = zips_init, 
    command = tigris::zctas(cb = FALSE)
  ),
  ### ...Companies ====
  targets::tar_target(
    name = companies_init,
    command = readr::read_csv(
      companies_file,
      n_max = config$company_count
      )
  ),
  ### ...Officers ====
  targets::tar_target(
    name = officers_init,
    command = readr::read_csv(officers_file) |>
      dplyr::filter(company_number %in% companies_init$company_number)
  ),
  ### ...Alternative Names ====
  targets::tar_target(
    name = alt_names_init,
    command = readr::read_csv(alt_names_file) |>
      dplyr::filter(company_number %in% companies_init$company_number)
  ),
  ### ...Municipalities ====
  targets::tar_target(
    name = munis_init,
    command = load_munis(state = config$state)
  ),
  ### ...States (`tigris`) ====
  targets::tar_target(
    name = states,
    command = tigris::states() |>
      st_preprocess(config$crs) |>
      dplyr::select(id = geoid, name, abbrev = stusps)
  ),
  ## Process... ====
  ### ...Municipalities ====
  targets::tar_target(
    name = munis,
    command = proc_munis(munis_init, crs = config$crs)
  ),
  ### ...ZIPs ====
  targets::tar_target(
    name = zips,
    command = proc_zips(
      zips_init, 
      state = config$state,
      munis = munis, 
      states = states, 
      crs = config$crs, 
      thresh = config$zip_int_thresh)
  ),
  ### ...Parcels ====
  targets::tar_target(
    name = parcels,
    command = proc_parcels(
      parcels_init,
      cbgs = cbgs,
      state = config$state,
      crs = config$crs
    )
  ),
  ### ...Assessor's table ====
  targets::tar_target(
    name = assess,
    command = proc_assess(
      assess_init,
      state = config$state
    )
  ),
  ### ...Companies ====
  targets::tar_target(
    name = companies,
    command = proc_companies(companies_init)
  ),
  ### ...Alternative Names ====
  targets::tar_target(
    name = alt_names,
    command = proc_alt_names(alt_names_init, companies = companies)
  ),
  ### ...Officers ====
  targets::tar_target(
    name = officers,
    command = proc_officers(officers_init)
  )
)