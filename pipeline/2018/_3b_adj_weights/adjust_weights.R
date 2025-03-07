## This script re-weights survey data to match to a given synthetic population

# provide config file as argument
args=(commandArgs(TRUE))
source(args)

countycorrfile <- "Q:/Model Development/ABM Transfer/R_Summaries/data/taz_districts_sfcta.csv"
countycorr <- read.csv(countycorrfile)

surveyhh <- read.table(file.path(surveydir,surveyhhfile),header=T)
hhvars <- names(surveyhh)
surveyper <- read.table(file.path(surveydir,surveyperfile),header=T)
pervars <- names(surveyper)
surveypday <- read.table(file.path(surveydir,surveypdayfile),header=T)
pdayvars <- names(surveypday)
surveytours <- read.table(file.path(surveydir,surveytourfile),header=T)
surveytrips <- read.table(file.path(surveydir,surveytripfile),header=T)

# DAY OF WEEK FILTRATION
# surveyhh <- surveyhh[surveyhh$hhdow %in% DOW,]
# surveyper <- surveyper[surveyper$hhno %in% surveyhh$hhno,]
# surveypday <- surveypday[surveypday$hhno %in% surveyhh$hhno,]
# surveytours <- surveytours[surveytours$hhno %in% surveyhh$hhno,]
# surveytrips <- surveytrips[surveytrips$hhno %in% surveyhh$hhno,]

classify_hh <- function(hhdf){
  hhdf$SIZECAT <- hhdf$hhsize
  hhdf$SIZECAT[hhdf$SIZECAT > 4] <- 4
  hhdf$INCCAT <- 1+findInterval(hhdf$hhincome,c(30000,60000,100000))
  hhdf$hhcounty <- countycorr$DISTRICT[match(hhdf$hhtaz,countycorr$TAZ)]
  return(hhdf)
}

agg_hh <- function(df, desc){
  df <- aggregate(df$hhexpfac,
                  list(hhcounty=df$hhcounty,SIZECAT=df$SIZECAT,INCCAT=df$INCCAT),sum,na.rm=T)
  names(df)[which(names(df)=="x")] <- desc
  return(df)
}

surveyhh <- classify_hh(surveyhh)
agg_survey <- agg_hh(surveyhh,"survey")
factors <- expand.grid(hhcounty=c(1:9),SIZECAT=c(1:4),INCCAT=c(1:4))
factors <- merge(factors,agg_survey,all.x=T)

if(CONTROL_SOURCE_DAYSIM & !CONTROL_SOURCE_SFSAMP){
  synth_hh <- read.table(file.path(synthdir,synth_hhfile),header=T)
  synth_per <- read.table(file.path(synthdir,synth_perfile),header=T)
  
#  # keep only adults
#  synth_per <- synth_per[synth_per$pagey>17,]

  #convert 1989 dollar to 2023 dollars (survey)
  synth_hh$hhincome <- synth_hh$hhincome*2.68
}

if(!CONTROL_SOURCE_DAYSIM & CONTROL_SOURCE_SFSAMP){
  synth_per <- read.table(file.path(synthdir,synth_perfile))
  col_names <- c('hhid','persid','sfzone','hhsize','hhadlt', 
                 'hh65up','hh5064','hh3549','hh2534','hh1824','hh1217','hhc511','hhchu5',
                 'hhfull','hhpart','hhvehs','hhinc','gender','age','relat','race','employ','educn')
  names(synth_per) <- col_names
  synth_per <- synth_per[order(synth_per$hhid, synth_per$persid),]
  
  synth_hh <- synth_per[!duplicated(synth_per$hhid),]
  synth_hh$hhtaz <- synth_hh$sfzone
  #convert 1989 dollar to 2009 dollars (survey)
  synth_hh$hhincome <- synth_hh$hhinc*1000*1.73
  synth_hh$hhexpfac <- 1
  
  synth_per$psexpfac <- 1
  ### define 4 person types - worker, student, other adult, child
  synth_per$child <- ifelse((synth_per$age<16) | (synth_per$age>=16 & synth_per$age<=20 & synth_per$educn>=1 & synth_per$educn<=5), 1, 0 )
  synth_per$adult <- 1-synth_per$child 
  synth_per$othadlt <- synth_per$adult * ifelse(synth_per$educn==0,1,0) * ifelse(synth_per$employ==5,1,0)
  synth_per$student <- synth_per$adult * (1-synth_per$othadlt) * ifelse(synth_per$educn>0,1,0) * ifelse(synth_per$employ>1,1,0)
  synth_per$worker <- synth_per$adult * (1-synth_per$othadlt-synth_per$student)
  synth_per$pptype <- synth_per$worker + 2*synth_per$student + 3*synth_per$othadlt + 4*synth_per$child
  
  surveyper$child <- ifelse((surveyper$pagey<16) | (surveyper$pagey>=16 & surveyper$pagey<=20 & surveyper$pstyp>0 & surveyper$pptyp>=6), 1, 0 )
  surveyper$adult <- 1-surveyper$child 
  surveyper$othadlt <- surveyper$adult * ifelse(surveyper$pstyp==0,1,0) * ifelse(surveyper$pwtyp==0,1,0)
  surveyper$student <- surveyper$adult * (1-surveyper$othadlt) * ifelse(surveyper$pstyp>0,1,0) * ifelse(surveyper$pwtyp!=1,1,0)
  surveyper$worker <- surveyper$adult * (1-surveyper$othadlt-surveyper$student)
  surveyper$pptyp_orig <- surveyper$pptyp
  surveyper$pptyp <- surveyper$worker + 2*surveyper$student + 3*surveyper$othadlt + 4*surveyper$child
}

# calculate hh weight adjustment factors
synth_hh <- classify_hh(synth_hh)
agg_synth <- agg_hh(synth_hh,"synth")
factors <- merge(factors,agg_synth,all.x=T)
factors$adjfac <- factors$synth/factors$survey
print("Household weight adjustment factors")
summary(factors$adjfac)
hist(factors$adjfac)
print(factors)

# update new hh weights in the hh file
surveyhh  <- merge(surveyhh,factors[,c("hhcounty","SIZECAT","INCCAT","adjfac")],all.x=T)
surveyhh$adjfac[is.na(surveyhh$adjfac)] <- 1
surveyhh$orig_hhexpfac <- surveyhh$hhexpfac
surveyhh$hhexpfac <- surveyhh$hhexpfac*surveyhh$adjfac
# write out the new hh file
surveyhh <- surveyhh[order(surveyhh$hhno),]
write.table(surveyhh[,c(hhvars,"adjfac","orig_hhexpfac")],file.path(outdir,outhhfile),row.names=F,quote=F)


surveyper <- merge(surveyper,surveyhh[,c("hhno","hhcounty","hhparcel","hhxco","hhyco")],all.x=T)
surveyper <- surveyper[surveyper$psexpfac>0,]
surveyper$orig_psexpfac <- surveyper$psexpfac

if(CONTROL_SOURCE_DAYSIM & !CONTROL_SOURCE_SFSAMP){
  surveypday$inpday <- 1
  temp_surveypday <- surveypday[,c("hhno","pno","inpday")]
  temp_surveypday <- temp_surveypday[!duplicated(temp_surveypday),]
  surveyper <- merge(surveyper,temp_surveypday,all.x=T)
  surveyper$psexpfac <- surveyper$psexpfac*surveyper$inpday
  
  synth_per <- merge(synth_per,synth_hh[,c("hhno","hhcounty")],all.x=T)
}

if(!CONTROL_SOURCE_DAYSIM & CONTROL_SOURCE_SFSAMP){
  synth_per <- merge(synth_per,synth_hh[,c("hhid","hhcounty")],all.x=T)
}

agg_per <- function(df, desc){
  df <- aggregate(df$psexpfac,
                  list(hhcounty=df$hhcounty,pptyp=df$pptyp),sum,na.rm=T)
  names(df)[which(names(df)=="x")] <- desc
  return(df)
}

# calculate person weight adjustment factors
agg_survey <- agg_per(surveyper,"survey")
factors <- expand.grid(hhcounty=c(1:9),pptyp=c(1:NUM_PTYPES))
factors <- merge(factors,agg_survey,all.x=T)
agg_synth <- agg_per(synth_per,"synth")
factors <- merge(factors,agg_synth,all.x=T)
factors$adjfac <- factors$synth/factors$survey
print("Person weight adjustment factors")
summary(factors$adjfac)
hist(factors$adjfac)
print(factors)

pervars <- c(pervars,"orig_psexpfac","adjfac")
surveyper  <- merge(surveyper,factors[,c("hhcounty","pptyp","adjfac")],all.x=T)
nrow(surveyper)
surveyper$adjfac[is.na(surveyper$adjfac)] <- 1
surveyper$psexpfac <- surveyper$psexpfac*surveyper$adjfac

tourcols <- names(surveytours)
tripcols <- names(surveytrips)

surveypday <- merge(surveypday,surveyper[,c("hhno","pno","adjfac")],all.x=T)
surveypday$pdexpfac <- surveypday$pdexpfac*surveypday$adjfac
if(nrow(surveypday[is.na(surveypday$pdexpfac),])>0)
  print("Problem in the pday file!!!!!")

surveytrips <- merge(surveytrips,surveyper[,c("hhno","pno","adjfac")],all.x=T)
surveytrips$trexpfac <- surveytrips$trexpfac*surveytrips$adjfac
if(nrow(surveytrips[is.na(surveytrips$trexpfac),])>0)
  print("Problem in the trip file!!!!!")

surveytours <- merge(surveytours,surveyper[,c("hhno","pno","adjfac")],all.x=T)
surveytours$toexpfac <- surveytours$toexpfac*surveytours$adjfac
if(nrow(surveytours[is.na(surveytours$toexpfac),])>0)
  print("Problem in the tour file!!!!!")

if(!CONTROL_SOURCE_DAYSIM & CONTROL_SOURCE_SFSAMP){
  # get the more detail pptyp back
  surveyper$pptyp <- surveyper$pptyp_orig
}

surveyper <- surveyper[,pervars]
surveyper <- surveyper[order(surveyper$hhno,surveyper$pno),]
surveypday <- surveypday[,pdayvars]
surveypday <- surveypday[order(surveypday$hhno,surveypday$pno),]
surveytrips <- surveytrips[,tripcols]
surveytrips <- surveytrips[order(surveytrips$hhno,surveytrips$pno,surveytrips$day,surveytrips$tour,surveytrips$half,surveytrips$tseg),]
surveytours <- surveytours[,tourcols]
surveytours <- surveytours[order(surveytours$hhno,surveytours$pno,surveytours$day,surveytours$tour),]


write.table(surveyper,file.path(outdir,outperfile),row.names=F,quote=F)
write.table(surveypday,file.path(outdir,outpdayfile),row.names=F,quote=F)
write.table(surveytours,file.path(outdir,outtourfile),row.names=F,quote=F)
write.table(surveytrips,file.path(outdir,outtripfile),row.names=F,quote=F)







