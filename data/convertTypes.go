package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pkg/errors"
)

var typeConvertions = map[string]string {
	"\"actor\"": "\"Person\"",
	"\"art_director\"": "\"Person\"",
	"\"casting_director\"": "\"Person\"",
	"\"cinematographer\"": "\"Person\"",
	"\"costumer_designer\"": "\"Person\"",
	"\"crewmember\"": "\"Person\"",
	"\"critic\"": "\"Person\"",
	"\"director\"": "\"Person\"",
	"\"editor\"": "\"Person\"",
	"\"music_contributor\"": "\"Person\"",
	"\"person_or_entity_appearing_in_film\"": "\"Person\"",
	"\"personal_appearance\"": "\"Person\"",
	"\"producer\"": "\"Person\"",
	"\"production_designer\"": "\"Person\"",
	"\"set_designer\"": "\"Person\"",
	"\"song_performer\"": "\"Person\"",
	"\"story_contributor\"": "\"Person\"",
	"\"theorist\"": "\"Person\"",
	"\"writer\"": "\"Person\"",

	"\"character\"": "\"Character\"",

	"\"collection\"": "\"Collection\"",

	"\"company\"": "\"Company\"",
	"\"company_role_or_service\"": "\"Company\"",
	"\"distributor\"": "\"Company\"",
	"\"festival_sponsor\"": "\"Company\"",
	"\"production_company\"": "\"Company\"",

	"\"content_rating\"": "\"ContentRating\"",

	"\"content_rating_system\"": "\"ContentRatingSystem\"",

	"\"crew_gig\"": "\"Job\"",
	"\"job\"": "\"Job\"",
	"\"special_performance_type\"": "\"Job\"",

	"\"cut\"": "\"Cut\"",

	"\"cut_type\"": "\"CutType\"",

	"\"distribution_medium\"": "\"DistributionMedium\"",

	"\"festival_focus\"": "\"FestivalFocus\"",

	"\"personal_appearance_type\"": "\"PersonalAppearanceType\"",

	"\"featured_song\"": "\"Song\"",
	"\"song\"": "\"Song\"",

	"\"festival\"": "\"Festival\"",

	"\"screening_venue\"": "\"Event\"",
	"\"festival_event\"": "\"Event\"",

	"\"film\"": "\"Movie\"",
	"\"dubbing_performance\"": "\"Movie\"",
	"\"regional_release_date\"": "\"Movie\"",

	"\"format\"": "\"Format\"",

	"\"genre\"": "\"Genre\"",

	"\"location\"": "\"Location\"",

	"\"performance\"": "\"Performance\"",

	"\"series\"": "\"Series\"",

	"\"subject\"": "\"Subject\"",
}

func convertTypes(path, outputPath string) error {
	readFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer readFile.Close()

	gzReader, err := gzip.NewReader(readFile)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)

	writeFile, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer writeFile.Close()

	gzWriter := gzip.NewWriter(writeFile)
	defer gzWriter.Close()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		subject := parts[0]
		predicate := parts[1]
		object := parts[2]
		if predicate == "<dgraph.type>" {
			newType, ok := typeConvertions[object]
			if !ok {
				return errors.Errorf("No conversion for type %s", object)
			}
			newLine := fmt.Sprintf("%s\t%s\t%s\t.\n", subject, predicate, newType)
			gzWriter.Write([]byte(newLine))
		} else {
			gzWriter.Write([]byte(line))
			gzWriter.Write([]byte("\n"))
		}
	}
	
	return nil
}

func main() {
	if err := convertTypes("21million.rdf.gz", "21million.rdf.gz.new"); err != nil {
		log.Fatal(err)
	}

}
