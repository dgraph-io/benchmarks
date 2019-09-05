package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pkg/errors"
)

var (
	file   = flag.String("file", "21million.rdf.gz", "Input RDF data file name.")
	output = flag.String("out", "21million-new.rdf.gz", "Output RDF data file name.")
)

var typeConvertions = map[string]string{
	"\"actor\"":                              "\"Actor\"",
	"\"art_director\"":                       "\"ArtDirector\"",
	"\"casting_director\"":                   "\"CastingDirector\"",
	"\"cinematographer\"":                    "\"Cinematographer\"",
	"\"costumer_designer\"":                  "\"CostumeDesigner\"",
	"\"crewmember\"":                         "\"CrewMember\"",
	"\"critic\"":                             "\"Critic\"",
	"\"director\"":                           "\"Director\"",
	"\"editor\"":                             "\"Editor\"",
	"\"music_contributor\"":                  "\"MusicContributor\"",
	"\"person_or_entity_appearing_in_film\"": "\"PersonOrEntityAppearingInFilm\"",
	"\"personal_appearance\"":                "\"PersonalAppearance\"",
	"\"producer\"":                           "\"Producer\"",
	"\"production_designer\"":                "\"ProductionDesigner\"",
	"\"set_designer\"":                       "\"SetDesigner\"",
	"\"song_performer\"":                     "\"SongPerformer\"",
	"\"story_contributor\"":                  "\"StoryContributor\"",
	"\"theorist\"":                           "\"Theorist\"",
	"\"writer\"":                             "\"Writer\"",

	"\"character\"": "\"Character\"",

	"\"collection\"": "\"Collection\"",

	"\"company\"":                 "\"Company\"",
	"\"company_role_or_service\"": "\"CompanyRoleOrService\"",
	"\"distributor\"":             "\"Distributor\"",
	"\"festival_sponsor\"":        "\"FestivalSponsor\"",
	"\"production_company\"":      "\"ProductionCompany\"",

	"\"content_rating\"": "\"ContentRating\"",

	"\"content_rating_system\"": "\"ContentRatingSystem\"",

	"\"crew_gig\"":                 "\"CrewGig\"",
	"\"job\"":                      "\"Job\"",
	"\"special_performance_type\"": "\"SpecialPerformanceType\"",

	"\"cut\"": "\"Cut\"",

	"\"cut_type\"": "\"CutType\"",

	"\"distribution_medium\"": "\"DistributionMedium\"",

	"\"festival_focus\"": "\"FestivalFocus\"",

	"\"personal_appearance_type\"": "\"PersonalAppearanceType\"",

	"\"featured_song\"": "\"FeaturedSong\"",
	"\"song\"":          "\"Song\"",

	"\"festival\"": "\"Festival\"",

	"\"screening_venue\"": "\"ScreeningVenue\"",
	"\"festival_event\"":  "\"FestivalEvent\"",

	"\"film\"":                  "\"Film\"",
	"\"dubbing_performance\"":   "\"DubbingPerformance\"",
	"\"performance\"":           "\"Performance\"",
	"\"regional_release_date\"": "\"RegionalReleaseDate\"",

	"\"format\"": "\"Format\"",

	"\"genre\"": "\"Genre\"",

	"\"location\"": "\"Location\"",

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
	flag.Parse()
	if err := convertTypes(*file, *output); err != nil {
		log.Fatal(err)
	}

}
