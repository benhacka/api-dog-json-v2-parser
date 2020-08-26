# api-dog-json-v2-parser
The parser for the saving photos from JSON dialogs that were generated with https://apidog.ru/

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]



<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About](#about)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Installation](#installation)
    * [Installation from pipy](#installation-from-pipy)
    * [Installation from source](#installation-from-source)
* [Usage](#usage)
  * [Help](#help)
  * [Use cases](#use-cases)
* [License](#license)
* [Thanks](#thanks)


<!-- ABOUT -->
## About

This is a simple lib for downloading photos from ApiDog's JSON with filtering. 
I think it's the fastest and safest method to backup photos from the dialogs (mail[dot]ru can f--k themself with API and official site).


### How to download JSON dialog
It's really ez. Get auth on https://apidog.ru/. Go to the dialog that you want to save. Click on the `action` and `save dialog in JSON`

<img src="https://github.com/benhacka/api-dog-json-v2-parser/blob/master/images/how_to_1.png" alt="HowTo1">
<img src="https://github.com/benhacka/api-dog-json-v2-parser/blob/master/images/how_to_2.png" alt="HowTo2">

<!-- GETTING STARTED -->
## Getting Started

You need a **python version of at least 3.6**.

### Installation

#### Installation from pipy

```pip install api-dog-json-v2-parser```

#### Installation from source
1.  Clone the repo
```sh
git clone https://github.com/benhacka/api-dog-json-v2-parser.git
```
2.  Go to the downloaded folder. 
```sh
cd api-dog-json-v2-parser
```
3.  Install
```sh
python install setup.py --user
```  
3.  Install with pip (alternative)
```sh
pip install .
```

<!-- USAGE EXAMPLES -->
## Usage

### Help
This is got from --help.

```
positional arguments:
  paths                 Path(s) for json scanning. Allowed . / <folder paths> Default "." - current dir

optional arguments:
  -h, --help            show this help message and exit
  -r, --recursive       Recursive walking flag. W/o the flag function is off
  -l LIMIT, --limit LIMIT
                        Download limit. Default value - 50
  -c {OWNER,OPPONENT,PAIR,ALL_EXCEPT_PAIR,ALL}, --collect {OWNER,OPPONENT,PAIR,ALL_EXCEPT_PAIR,ALL}
                        Grabbing filter. By default - ALL. owner - grab only owner photos (info from meta).
                        opponent - grab only opponent photos (info from meta). pair - grab owner and opponent
                        photos (info from meta). all_except_pair - grab all except photos of owner and
                        opponent (it is grabbing forwarding photos in fact). Can be useful if some one forward
                        "leaked" content. all - grab all photos from dialog (groups photo albums excluded).
  -n, --dont-get-names  Default: try to get real name from vk and write it into the folder name. With the flag
                        folder will be contain only id (don't send get request on the VK servers -> it's a
                        little bit faster)
  --json-name
  --wo-sub-folder
  --custom-name CUSTOM_NAME
                        Name of the future folder
```

### Use cases



1.  Typical use case: download photos from all json in the folder. ```cd %folder%``` in your terminal. It's running script with default args.

```sh
api-dog-pv2
```

2.  Download photos from all JSON's in the folder.
```sh
api-dog-pv2 ~/.my-dirty-secrete/stoled-mom-photos
```

3.  Download only companion photos.
```sh
api-dog-pv2 ~/.stop-using-vk/dialogs_with_Polina -c OPPONENT
```

4.  Download forwarded photos.
```sh
api-dog-pv2 ~/.my-company-dialogs/hard-party -c ALL_EXCEPT_PAIR
```

5.  Download all your ~(not stolen photos of course ;) )~ photos from different dialogs.
```sh
api-dog-pv2 ~/.hack-place/stoled_Natashas_photos -c OWNER --custom-name to_do___sort_nude
```

6.  Recursive download in the semit-root folder (photos id folders will be near JSON)
```sh
api-dog-pv2 ~/.large-folder -r --wo-sub-folder
```

7.  Download w/o real name parsing (w/o request on the m.vk.com) with max thread count - 499.
```sh
api-dog-pv2 ~/.last-case -n -l 499
```

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Thanks
`Привет, мы - команда APIdog. Вот уже почти 8 лет (с 8 августа 2012 года) мы разрабатываем и держим этот сайт.`  
Thanks a lot to API-DOG dev - @vladislav805, it's a really great alternative of official VK. You can support him (as me) if you like his work (as me).


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/benhacka/toruploader.svg?style=flat-square
[contributors-url]: https://github.com/benhacka/toruploader/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/benhacka/toruploader.svg?style=flat-square
[forks-url]: https://github.com/benhacka/toruploader/network/members
[stars-shield]: https://img.shields.io/github/stars/benhacka/toruploader.svg?style=flat-square
[stars-url]: https://github.com/benhacka/toruploader/stargazers
[issues-shield]: https://img.shields.io/github/issues/benhacka/toruploader.svg?style=flat-square
[issues-url]: https://github.com/benhacka/toruploader/issues
[license-shield]: https://img.shields.io/github/license/benhacka/toruploader.svg?style=flat-square
[license-url]: https://github.com/benhacka/toruploader/blob/master/LICENSE.txt
