<?php

//Read the file links.csv and build the relations to the mysql database for all the sentences


//Prepare db connection:
$db = new PDO('mysql:host=localhost;dbname=tatoeba;charset=utf8', 'root', 'root');

$arrMeaningIds = array(); //keys are sentence ids
function fetchMeaningIds(){
	global $db;
	global $arrMeaningIds;

	$intRowCounter = 0;
	$intLimit = 50000;
	while(true){
		echo("Fetching rows limit($intRowCounter, $intLimit)\n");
		$stmt = $db->prepare("SELECT id, meaning_id FROM sentences WHERE meaning_id IS NOT NULL LIMIT $intRowCounter , $intLimit ;");
		$stmt->execute();
		$arrRows = $stmt->fetchAll(PDO::FETCH_ASSOC);
		foreach($arrRows as $arrRow){
			$arrMeaningIds[$arrRow['id']] = $arrRow['meaning_id'];
		}
		$intRowCounter += count($arrRows);
		if(count($arrRows) < $intLimit){ //it has reached the end of the db
			break;
		}
	}
}
fetchMeaningIds();
echo("Fetched ".count($arrMeaningIds)." meaning ids from db\n");

function setMeaningIds($arrLines){
	global $db;
	global $arrMeaningIds;

	//insert into sentences (id, meaning_id) values(1, 'mymeaning1'),(2, 'mymeaning2') on duplicate key update meaning_id = values(meaning_id)
	$strSql = "INSERT INTO sentences (id, meaning_id) VALUES ";
	$arrValues = array();
	foreach($arrLines as $arrSentenceIds){
		//Check that we don't have a unique id for those sentences:
		$strUniqueId = substr(md5(mcrypt_create_iv(16, MCRYPT_DEV_URANDOM)), 0, 16); //default if both are empty
		$blnIgnoreFirst = $blnIgnoreSecond = false;
		if(!empty($arrMeaningIds[$arrSentenceIds[0]])){
			$strUniqueId = $arrMeaningIds[$arrSentenceIds[0]];
			$blnIgnoreFirst = true;
		}
		if(!empty($arrMeaningIds[$arrSentenceIds[1]])){
			$strUniqueId = $arrMeaningIds[$arrSentenceIds[1]];
			$blnIgnoreSecond = true;
		}
		if(!$blnIgnoreFirst){
			$strSql .= "(?,?),";
			$arrValues[] = $arrSentenceIds[0];
			$arrValues[] = $strUniqueId;
			$arrMeaningIds[$arrSentenceIds[0]] = $strUniqueId;
		}
		if(!$blnIgnoreSecond){
			$strSql .= "(?,?),";
			$arrValues[] = $arrSentenceIds[1];
			$arrValues[] = $strUniqueId;
			$arrMeaningIds[$arrSentenceIds[1]] = $strUniqueId;
		}
	}
	$strSql = rtrim($strSql, ","); //would cut trailing commas.
	$strSql .= " ON DUPLICATE KEY UPDATE meaning_id = VALUES(meaning_id);";
	
	echo("Executing query with ".count($arrValues)." values\n");
	if(!empty($arrValues)){
		$stmt = $db->prepare($strSql);
		$stmt->execute($arrValues);
	}
	return;
}


function microtime_float()
{
    list($usec, $sec) = explode(" ", microtime());
    return ((float)$usec + (float)$sec);
}

//Open the file:
$handle = fopen("links.csv", "r");
if ($handle) {
	//Initialize in line offset:
	echo("Going to line offset\n");
	$intLineOffset = 6000000 + 340000;
	$intLineCounter = 0;
	while($intLineCounter < $intLineOffset){
		$strLine = fgets($handle);
		if($strLine === false){
			die("Unexpected end of file while setting the pointer to initial position\n");
		}
		$intLineCounter++;
	}
	//Now the pointer of the file is in the correct position
	//For each line of the file:
	echo("Reading file\n");
	$intNumLines = 10000;
    while (!feof($handle)) {
    	//$strRead = fread($handle, 100000);
    	$arrLines = array();
    	for ($i=0; $i < $intNumLines; $i++) { 
    		$strLine = fgets($handle);
    		if($strLine === false){
    			break; //eof
    		}
    		if(!empty($strLine)){
    			$arrLine = explode("\t", $strLine);
    			if(count($arrLine) === 2){
    				$arrLines[] = $arrLine;
    			}
    		}
    	}
    	
    	setMeaningIds($arrLines);
    	$intLineCounter += count($arrLines);

    	echo("Lines: $intLineCounter\n");
    }
    echo "$intLineCounter lines have been processed, EOF\n";
    fclose($handle);
} else {
    die('error opening the file');
} 
