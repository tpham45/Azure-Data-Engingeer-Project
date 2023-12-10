# Load the libraries
library(dplyr)
library(ggplot2)
library(ggrepel)
library(maps)

# Get world map data which includes the 'centre' of each country
world_map_data <- map_data("world")

# Extract country centers
country_centers <- world_map_data %>%
  group_by(region) %>%
  summarise(long = mean(long), lat = mean(lat))

# Merge country centers with your athmedals data frame
# Note: You might need to adjust the country names to match exactly.
athmedals_with_coords <- athmedals %>%
  left_join(country_centers, by = c("Country" = "region"))

# Now you can plot the top 10 countries on the map
top_countries <- athmedals_with_coords %>%
  arrange(desc(Total)) %>%
  slice_head(n = 10)

athmedals_filtered <- athmedals_with_coords %>%
  filter(is.na(long) & is.na(lat))
