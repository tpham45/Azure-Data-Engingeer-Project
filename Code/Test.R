# Load the libraries
library(dplyr)
library(ggplot2)
library(reshape2)
library(melt)
#Medals
# Most gold medals of top 10 countries
top_countries_gold <- medals %>%
  arrange(desc(Gold)) %>%
  slice(1:10)

ggplot(top_countries_gold, aes(x=reorder(Country, Gold), y=Gold)) +
  geom_bar(stat="identity", fill="gold") +
  coord_flip() + # Flipping coordinates for horizontal bars
  geom_text(aes(label=Gold), position=position_dodge(width=0.9), hjust=-0.1, size=3.5) +
  labs(x="Country", y="Gold Medals", title="Top 10 Countries by Gold Medal")


# Most every medals of top 10 countries
top_countries_overall <- athmedals %>%
  arrange(desc(Total)) %>%
  head(10)
ggplot(top_countries_overall, aes(x=reorder(Country, Total), y=Total)) +
  geom_bar(fill="steelblue", stat="identity") +
  coord_flip() + # Flipping coordinates for horizontal bars
  geom_text(aes(label=Total), position=position_dodge(width=0.9), hjust=-0.1, size=3.5) +
  labs(x="Country", y="Total Medals", title="Top 10 Countries by Total Medal Tally")



# Heatmap of medals
athmedals_melted <- melt(top_countries_overall, id.vars='Country', measure.vars=c('Gold', 'Silver', 'Bronze'))
ggplot(athmedals_melted, aes(x=Country, y=variable, fill=value)) +
  geom_tile() +
  scale_fill_gradient(low="white", high="blue") +
  theme(axis.text.x = element_text(angle=90, hjust=1)) +
  labs(x="Country", y="Medal Type", fill="Count", title="Heatmap of Medals by Country")



###################################################################
##Gender distribution on each discipline
# Female on each discipline
ggplot(entriesgender, aes(x=Discipline, y=Female, fill=Female)) +
  geom_bar(stat="identity", position="dodge") +
  theme(axis.text.x = element_text(angle=65, hjust=1)) +
  labs(x="Discipline", y="Number of Athletes", title="Gender Distribution Across Disciplines")

# Male on each discipline
ggplot(entriesgender, aes(x=Discipline, y=Male, fill=Male)) +
  geom_bar(stat="identity", position="dodge") +
  theme(axis.text.x = element_text(angle=65, hjust=1)) +
  labs(x="Discipline", y="Number of Athletes", title="Gender Distribution Across Disciplines")


######################################################################

# First, group by discipline and summarize the unique number of countries
discipline_participation <- athletes %>%
  group_by(Discipline) %>%
  summarise(Countries = n_distinct(Country)) %>%
  arrange(desc(Countries)) %>%
  slice(1:10) # Selects the top 10 disciplines with the most unique countries

# top 10 Disciplines by countries
ggplot(discipline_participation, aes(x=reorder(Discipline, Countries), y=Countries)) +
  geom_bar(stat="identity", fill="steelblue") +
  coord_flip() + # Flipping coordinates for horizontal bars
  geom_text(aes(label=Countries), position=position_dodge(width=0.9), hjust=-0.1, size=3.5) +
  labs(x="Discipline", y="Number of Countries", title="Top 10 Disciplines by Country Participation")
