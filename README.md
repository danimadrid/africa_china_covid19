## Replication materials for the paper "Who Set the Narrative? Assessing the Influence of Chinese Global Media on News Coverage of COVID-19 in 30 African Countries"
<br>
This repository includes materials (processed data and code) used for the analysis in the paper "Who Set the Narrative? Assessing the Influence of Chinese Global Media on News Coverage of COVID-19 in 30 African Countries" which was published in <em><a href="https://journals.sagepub.com/doi/full/10.1177/20594364211013714" target="_blank">Global Media and China</em></a>.
<br>
<br>
<strong><em>Abstract</em></strong><br>
<p>The size of China’s State-owned media’s operations in Africa has grown significantly since the early 2000s. Previous research on the impact of increased Sino-African mediated engagements has been inconclusive. Some researchers hold that public opinion towards China in African nations has been improving because of the increased media presence. Others argue that the impact is rather limited, particularly when it comes to affecting how African media cover China-related stories. This paper contributes to this debate by exploring the extent to which news media in 30 African countries relied on Chinese news sources to cover China and the COVID-19 outbreak during the first half of 2020. By computationally analyzing a corpus of 500,000 written news stories, I show that, compared to other major global players (e.g. Reuters, AFP), content distributed by Chinese media (e.g. Xinhua, China Daily) is much less likely to be used by African news organizations, both in English and French speaking countries. The analysis also reveals a gap in the prevailing themes in Chinese and African media’s coverage of the pandemic. The implications of these findings for the sub-field of Sino-African media relations, and the study of global news flows are discussed.</p>
<br>
<strong>Content</strong><br>
<p>This repository includes the following scripts:</p>
<ul>
  <li>1_NewsAPI.R: used to extract data from the News API service [text data not included] </li>
  <li>2_NexisParser.R: used to process content from the Nexis Uni database for analysis [text data not included]</li>  
  <li>3_GDELT.R: used to extract URLs from the GDELT database and to scrape the content of news items</li>
  <li>4_ReutersScraper.R: used to search for Reuters articles on Google (using serpAPI) and scraped news content</li>
  <li>5_MergingCleaning.R: used to combine, clean and pre-process data from all sources</li>
  <li>6_Analysis.R: used to analyse the data; the file sis organised by research question (RQ1 to RQ4)</li>
  <li>7_OnlineAppendices.R: used to create the data summaries presented in the Online Appendices</li>
</ul>
<br>
The data files used in this study are available at <a href="https://doi.org/10.18738/T8/FNYGHO" target="_blank">https://doi.org/10.18738/T8/FNYGHO</a>.<br>
<br>
Due to copyright restrictions, the original news articles cannot be shared and, therefore, only pre-processed data (i.e., document-feature matrices) and non-copyrighted data are provided in the repository.
