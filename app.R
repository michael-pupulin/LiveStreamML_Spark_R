#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#
library(shiny)
library(shinydashboard)
library(ggplot2)
library(lubridate)
library(smooth)
library(data.table)
library(tidyr)
library(forecast)
library(purrr)

# Define UI for application that draws a histogram
ui <- dashboardPage(
  skin = "black",
  dashboardHeader(title = 'Delivery Time Stream'),
  dashboardSidebar(sidebarMenu(
    menuItem("Home", tabName = "home"),
    menuItem('About', tabName = 'about')
  )),
  
  dashboardBody(
    tabItems(
      tabItem(tabName = "home",
              fluidRow(box(dataTableOutput("latest"), width = 12)),
              fluidRow(box(plotOutput("bar1"), width = 6),box(plotOutput("ts"), width = 6)),
              ),
      tabItem(tabName = "about",
              fluidRow(box("Here I can talk about the model or code and provide links etc.", width = 12)),
      )
    )
  )
  
)

# Define server logic required to draw a histogram
server <- function(input, output,session) {

  
  df <- read.csv("~/Desktop/SparkShiny/Classify/Train.csv/part-00000-4d60d1b1-913b-4b74-9871-13bcefe3fe60-c000.csv",header = TRUE)
  
  Ship = df[which(df$Mode_of_Shipment == "Ship"),]
  Flight = df[which(df$Mode_of_Shipment == "Flight"),]
  Road = df[which(df$Mode_of_Shipment == "Road"),]
  
  
  
  md <- data.frame(
    Mode = c("Ship","Flight","Road"),
    Lates = c(100*sum(Ship$Reached.on.Time_Y.N)/length(Ship$Reached.on.Time_Y.N),
              100*sum(Flight$Reached.on.Time_Y.N)/length(Flight$Reached.on.Time_Y.N),
              100*sum(Road$Reached.on.Time_Y.N)/length(Road$Reached.on.Time_Y.N))
  )
  

  
  df_1 <- reactivePoll(
    1000, session,
    checkFunc = function(){
      
      if(is_empty(      files<-list.files(path = "/Users/michaelpupulin/Desktop/SparkShiny/Classify/StreamOutput", pattern = "*.csv", full.names = TRUE)))
      {""}
      else {setwd("/Users/michaelpupulin/Desktop/SparkShiny/Classify/StreamOutput")
        files<-list.files(path = "/Users/michaelpupulin/Desktop/SparkShiny/Classify/StreamOutput", pattern = "*.csv", full.names = TRUE) 
        # this function returns the most recent modification time in the folder
        info <- file.info(files)
        max(info$mtime)}
    },
    valueFunc = function(){
      # this function returns the content of the most recent file in the folder
      files <- list.files("/Users/michaelpupulin/Desktop/SparkShiny/Classify/StreamOutput",pattern = "*.csv", full.names = TRUE)
      info <- file.info(files)
      i <- which.max(info$mtime)
      setwd("/Users/michaelpupulin/Desktop/SparkShiny/Classify/StreamOutput")
      res<-read.csv(files[i])
      colnames(res)<-c("Order ID","Prediction (On Time=0, Late=1)", "Timestamp")
      res
    }
  )

  
  
  output$latest <-renderDataTable({df_1()},options = list(pageLength = 5))
  
  # Basic barplot
  p<-ggplot(data=md, aes(x=Mode, y=Lates,fill=Mode)) +
    geom_bar(stat="identity") + ggtitle("Percentage of deliveries late by shipment mode") +
    xlab("") + ylab("Lates (%)") + coord_flip()
  
  setwd("/Users/michaelpupulin/Desktop/SparkShiny/Classify")
  t<- read.csv("ts.csv", header = TRUE)
  
  timeseries<- ts(t$Lates,frequency = 2,start = c(2001))
  model<- HoltWinters(timeseries)
  
  myforecast <- forecast(model, level= c(95, 99), h=15)

  
  output$bar1 <- renderPlot(p)
  
  output$ts <- renderPlot(
    plot(myforecast, xlab = "Year", ylab = "Number of late packages", main = "HoltWinters Forecast")
  )
  
}

# Run the application 
shinyApp(ui = ui, server = server)
