"""
enrichment.py — Product description enrichment.

Two-layer system:
1. Pattern decoder: Decodes ESAB abbreviated descriptions into human-readable text.
   Handles all welding consumables (wire, rod, electrodes) and common hardgoods.
2. Web cache: Reads pre-fetched Google/web search descriptions from data/descriptions.json.

The enriched description is used for both display and search matching.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


# ── ESAB Product Line Lookup ───────────────────────────────
# Maps abbreviated product line codes to full names + type

PRODUCT_LINES: dict[str, tuple[str, str]] = {
    # (full_name, product_type)
    # MIG/GMAW wire
    "WELD 70S 6":       ("Spoolarc Weld 70S-6",        "MIG Wire (ER70S-6)"),
    "WELD 70S 3":       ("Spoolarc Weld 70S-3",        "MIG Wire (ER70S-3)"),
    "86":               ("Spoolarc 86",                 "MIG Wire (ER70S-6)"),
    "86E":              ("Spoolarc 86",                 "MIG Wire (ER70S-6)"),
    "86 E":             ("Spoolarc 86",                 "MIG Wire (ER70S-6)"),
    "29S":              ("Spoolarc 29S",                "MIG Wire (ER80S-D2)"),
    "81":               ("Arcaloy 81",                  "MIG/TIG Wire"),
    "81 3":             ("Arcaloy 81-B3",               "Chrome-Moly Wire"),
    "81 5":             ("Arcaloy 81-B5",               "Chrome-Moly Wire"),
    "83":               ("Spoolarc 83",                 "MIG Wire (ER80S-Ni1)"),
    "83E":              ("Spoolarc 83",                 "MIG Wire (ER80S-Ni1)"),
    "71":               ("Spoolarc 71",                 "MIG Wire"),
    "71 5":             ("71-5",                        "MIG Wire"),
    "82":               ("Spoolarc 82",                 "MIG Wire (ER80S-D2)"),
    "82 E":             ("Spoolarc 82",                 "MIG Wire (ER80S-D2)"),
    "95":               ("Spoolarc 95",                 "MIG Wire"),
    "95E":              ("Spoolarc 95",                 "MIG Wire"),
    "95 E":             ("Spoolarc 95",                 "MIG Wire"),
    "95 CU":            ("Spoolarc 95 CU",              "Copper-Coated MIG Wire"),
    "120":              ("Spoolarc 120",                "Submerged Arc Wire"),
    "120E":             ("Spoolarc 120",                "Submerged Arc Wire"),
    "120 CU":           ("Spoolarc 120 CU",             "Copper-Coated Submerged Arc Wire"),
    "65":               ("Spoolarc 65",                 "Submerged Arc Wire"),
    "65 E":             ("Spoolarc 65",                 "MIG Wire"),
    "65E":              ("Spoolarc 65",                 "MIG Wire"),
    "75E":              ("Spoolarc 75",                 "MIG Wire"),
    "75 E":             ("Spoolarc 75",                 "MIG Wire"),
    "CW":               ("CoreWeld",                    "Metal-Cored Wire"),
    "CW 70":            ("CoreWeld 70",                 "Metal-Cored Wire (E70C-6M)"),
    "CW C6":            ("CoreWeld C6",                 "Metal-Cored Wire"),
    # Flux-cored wire
    "DS":               ("Dual Shield",                 "Flux-Cored Wire"),
    "DS II":            ("Dual Shield II",              "Flux-Cored Wire"),
    "DS II 70":         ("Dual Shield II 70",           "Flux-Cored Wire (E71T-1)"),
    "DS II 70 ULT":     ("Dual Shield II 70 Ultra",     "Flux-Cored Wire (E71T-1)"),
    "DS II 71":         ("Dual Shield II 71",           "Flux-Cored Wire"),
    "DS II 71 ULT":     ("Dual Shield II 71 Ultra",     "Flux-Cored Wire (E71T-1)"),
    "DS 710X":          ("Dual Shield 710X",            "Flux-Cored Wire"),
    "DS 7100 ULT":      ("Dual Shield 7100 Ultra",      "Flux-Cored Wire (E71T-1M)"),
    "DS 7100 LC":       ("Dual Shield 7100 Low Carbon", "Flux-Cored Wire"),
    "DS 7100 SR":       ("Dual Shield 7100 SR",         "Flux-Cored Wire"),
    "DS 111RB":         ("Dual Shield 111RB",           "Flux-Cored Wire"),
    "DS T":             ("Dual Shield T",               "Flux-Cored Wire"),
    "DS T 75":          ("Dual Shield T-75",            "Flux-Cored Wire"),
    "DS R 70":          ("Dual Shield R 70",            "Flux-Cored Wire"),
    "DS R 70 ULT":      ("Dual Shield R 70 Ultra",      "Flux-Cored Wire"),
    "ESAB 71":          ("ESAB 71",                     "Flux-Cored Wire"),
    # Self-shielded flux-cored
    "CS":               ("Coreshield",                  "Self-Shielded Flux-Cored Wire"),
    "CS 8":             ("Coreshield 8",                "Self-Shielded Flux-Cored Wire (E71T-8)"),
    "CS 11":            ("Coreshield 11",               "Self-Shielded Flux-Cored Wire (E71T-11)"),
    "CS 15":            ("Coreshield 15",               "Self-Shielded Flux-Cored Wire (E71T-14)"),
    # Stick electrodes — Atom Arc
    "AA":               ("Atom Arc",                    "Stick Electrode"),
    "AA 7018":          ("Atom Arc 7018",               "Stick Electrode (E7018)"),
    "AA 7018 1":        ("Atom Arc 7018-1",             "Stick Electrode (E7018-1)"),
    "AA 7018 M":        ("Atom Arc 7018 Moisture Resistant", "Stick Electrode (E7018-H4R)"),
    "AA T":             ("Atom Arc T",                  "Stick Electrode"),
    # Stick electrodes — Sureweld (abbreviated SW)
    "SW":               ("Sureweld",                    "Stick Electrode"),
    "SW T":             ("Sureweld T",                  "Stick Electrode"),
    "SW T 11":          ("Sureweld T-11",               "Stick Electrode (E6011)"),
    # Stick electrodes — Sureweld (full name)
    "SUREWELD 6010":    ("Sureweld 6010",               "Stick Electrode (E6010)"),
    "SUREWELD 6011":    ("Sureweld 6011",               "Stick Electrode (E6011)"),
    "SUREWELD 6013":    ("Sureweld 6013",               "Stick Electrode (E6013)"),
    "SUREWELD 7014":    ("Sureweld 7014",               "Stick Electrode (E7014)"),
    "SUREWELD 7018":    ("Sureweld 7018",               "Stick Electrode (E7018)"),
    "SUREWELD 7024":    ("Sureweld 7024",               "Stick Electrode (E7024)"),
    "SUREWELD 308L 16": ("Sureweld 308L-16",            "Stainless Stick Electrode (E308L-16)"),
    "SUREWELD 309L 16": ("Sureweld 309L-16",            "Stainless Stick Electrode (E309L-16)"),
    "SUREWELD 316L 16": ("Sureweld 316L-16",            "Stainless Stick Electrode (E316L-16)"),
    "SUREWELD 308LSI MIG": ("Sureweld 308LSi MIG",      "Stainless MIG Wire (ER308LSi)"),
    # Submerged arc
    "AS":               ("AutoShield",                  "Submerged Arc Wire"),
    "SA":               ("Spoolarc",                    "Submerged Arc Wire"),
    # A S = Arcaloy Stainless stick electrodes
    "A S SUPER 4 60":   ("Arcaloy Super 4-60",          "Stainless Stick Electrode"),
    "A S 4 60":         ("Arcaloy 4-60",                "Stainless Stick Electrode"),
    "A S SMOOTHCOTE 34":("Arcaloy Smoothcote 34",       "Stainless Stick Electrode"),
    "A S 275":          ("Arcaloy 275",                 "Stainless Stick Electrode"),
    "A S STEELARC 80LV":("Arcaloy Steelarc 80LV",      "Low-Alloy Stick Electrode"),
    "A S HS 2C":        ("Arcaloy HS-2C",               "Stainless Stick Electrode"),
    "A S 8 60":         ("Arcaloy 8-60",                "Stainless Stick Electrode"),
    "A S":              ("Arcaloy Stainless",            "Stick Electrode"),
    # OK brand (ESAB Europe)
    "OK FLUX 10":       ("OK Flux 10",                  "Submerged Arc Flux"),
    "OK FLUXN 10":      ("OK Flux 10",                  "Submerged Arc Flux"),
    "OK AUTROD 308LSI": ("OK Autrod 308LSi",            "Stainless MIG Wire"),
    "OK AUTROD 12":     ("OK Autrod 12.50/12.51",       "MIG Wire (ER70S-6)"),
    "OK AUTROD":        ("OK Autrod",                   "MIG/TIG Wire (ESAB Europe)"),
    "OK TIGROD":        ("OK Tigrod",                   "TIG Rod (ESAB Europe)"),
    "OK TUBROD 14 12":  ("OK Tubrod 14.12",             "Flux-Cored Wire (ESAB Europe)"),
    "OK TUBROD":        ("OK Tubrod",                   "Flux-Cored Wire (ESAB Europe)"),
    "OK AR12 62":       ("OK Autrod 12.62",             "Low-Alloy MIG Wire"),
    "OK AR12 63":       ("OK Autrod 12.63",             "MIG Wire"),
    "OK AR12 50":       ("OK Autrod 12.50",             "MIG Wire (ER70S-6)"),
    "OK AR 12 50":      ("OK Autrod 12.50",             "MIG Wire (ER70S-6)"),
    "OK AR 12 63":      ("OK Autrod 12.63",             "MIG Wire"),
    "OK AR55":          ("OK Autrod 55",                "Low-Alloy MIG Wire"),
    "OK AR13 26":       ("OK Autrod 13.26",             "Stainless MIG Wire"),
    "OK 53 70":         ("OK 53.70",                    "Stick Electrode (E7016)"),
    "OK 12 51":         ("OK 12.51",                    "MIG Wire (ER70S-6)"),
    "OK 12 50":         ("OK 12.50",                    "MIG Wire (ER70S-6)"),
    "OK":               ("OK (ESAB Europe)",            "Welding Consumable"),
    # Aluminum — ER prefix
    "ER4043":           ("Alcotec ER4043",              "Aluminum MIG/TIG Wire (4043)"),
    "ER5356":           ("Alcotec ER5356",              "Aluminum MIG/TIG Wire (5356)"),
    "ER1100":           ("Alcotec ER1100",              "Aluminum MIG/TIG Wire (1100)"),
    "ER4047":           ("Alcotec ER4047",              "Aluminum MIG/TIG Wire (4047)"),
    "ER5183":           ("Alcotec ER5183",              "Aluminum MIG/TIG Wire (5183)"),
    "ER5554":           ("Alcotec ER5554",              "Aluminum MIG/TIG Wire (5554)"),
    "ER5556":           ("Alcotec ER5556",              "Aluminum MIG/TIG Wire (5556)"),
    "ER4145":           ("Alcotec ER4145",              "Aluminum MIG/TIG Wire (4145)"),
    # Aluminum — R prefix (TIG rod)
    "R4043":            ("Alcotec R4043",               "Aluminum TIG Rod (4043)"),
    "R4047":            ("Alcotec R4047",               "Aluminum TIG Rod (4047)"),
    "R4009":            ("Alcotec R4009",               "Aluminum TIG Rod (4009)"),
    "R5356":            ("Alcotec R5356",               "Aluminum TIG Rod (5356)"),
    "R5183":            ("Alcotec R5183",               "Aluminum TIG Rod (5183)"),
    "R5556":            ("Alcotec R5556",               "Aluminum TIG Rod (5556)"),
    "R5554":            ("Alcotec R5554",               "Aluminum TIG Rod (5554)"),
    "R1100":            ("Alcotec R1100",               "Aluminum TIG Rod (1100)"),
    # Aluminum — bare number
    "4043":             ("Alcotec 4043",                "Aluminum MIG/TIG Wire (4043)"),
    "5356":             ("Alcotec 5356",                "Aluminum MIG/TIG Wire (5356)"),
    # Stainless — Exaton
    "EXATON ER308 308L":    ("Exaton ER308/308L",       "Stainless MIG/TIG Wire"),
    "EXATON ER308SI 308LSI":("Exaton ER308Si/308LSi",   "Stainless MIG/TIG Wire"),
    "EXATON ER316 316L":    ("Exaton ER316/316L",       "Stainless MIG/TIG Wire"),
    "EXATON ER316SI 316LSI":("Exaton ER316Si/316LSi",   "Stainless MIG/TIG Wire"),
    "EXATON ER309 309L":    ("Exaton ER309/309L",       "Stainless MIG/TIG Wire"),
    "EXATON ER309LSI":      ("Exaton ER309LSi",         "Stainless MIG/TIG Wire"),
    "EXATON ER2209":        ("Exaton ER2209",           "Duplex Stainless Wire/Rod"),
    "EXATON ER2594":        ("Exaton ER2594",           "Super Duplex Stainless Wire/Rod"),
    "EXATON ER25 22 2 LMN": ("Exaton ER25-22-2 LMn",   "Super Duplex Stainless Wire/Rod"),
    "EXATON ER317L":        ("Exaton ER317L",           "Stainless MIG/TIG Wire (317L)"),
    "EXATON AXT":           ("Exaton AXT",              "Stainless Wire/Rod"),
    "EXATON":               ("Exaton",                  "Stainless Wire/Rod"),
    # Stainless — Arcaloy stick
    "AC 309L 16":       ("Arcaloy 309L-16",             "Stainless Stick Electrode (E309L-16)"),
    "AC 309L 15":       ("Arcaloy 309L-15",             "Stainless Stick Electrode (E309L-15)"),
    "AC 309 309H 16":   ("Arcaloy 309/309H-16",         "Stainless Stick Electrode (E309-16)"),
    "AC 309NB 16":      ("Arcaloy 309Nb-16",            "Stainless Stick Electrode (E309Nb-16)"),
    "AC 316LF5 16":     ("Arcaloy 316LF5-16",           "Stainless Stick Electrode (E316L-16)"),
    "AC 308L":          ("Arcaloy 308L",                "Stainless Wire/Rod"),
    "AC 309L":          ("Arcaloy 309L",                "Stainless Stick Electrode"),
    # Stainless — Shield-Brite
    "SB 308L":          ("Shield-Brite 308L",           "Stainless MIG Wire (ER308L)"),
    "SB 308H":          ("Shield-Brite 308H",           "Stainless MIG Wire (ER308H)"),
    "SB 309L":          ("Shield-Brite 309L",           "Stainless MIG Wire (ER309L)"),
    "SB 316L":          ("Shield-Brite 316L",           "Stainless MIG Wire (ER316L)"),
    "SB":               ("Shield-Brite",                "Stainless MIG Wire"),
    # Nickel alloy
    "NCU80SB2":         ("Arcaloy NCU80SB2",           "Nickel Alloy Wire/Rod"),
    "NCU80SB6":         ("Arcaloy NCU80SB6",           "Nickel Alloy Wire/Rod"),
    "NCU80SB8":         ("Arcaloy NCU80SB8",           "Nickel Alloy Wire/Rod"),
    "NCU90SB3":         ("Arcaloy NCU90SB3",           "Nickel Alloy Wire/Rod"),
    "NCU90SB9":         ("Arcaloy NCU90SB9",           "Nickel Alloy Wire/Rod"),
    "TIG80SB2":         ("Arcaloy TIG80SB2",           "Nickel Alloy TIG Rod"),
    "TIG90SB3":         ("Arcaloy TIG90SB3",           "Nickel Alloy TIG Rod"),
    "NC 55":            ("Arcaloy NC-55",               "Nickel Alloy Wire/Rod"),
    # Copper-coated / specialty MIG
    "PURUS 42":         ("Purus 42",                    "Copper-Free MIG Wire (ER70S-6)"),
    # TIG rod
    "WELD 70S 6 TIG":  ("Spoolarc Weld 70S-6 TIG",    "TIG Rod (ER70S-6)"),
    "WELD 70S 3 TIG":  ("Spoolarc Weld 70S-3 TIG",    "TIG Rod (ER70S-3)"),
    # Additional flux-cored
    "DS 710":           ("Dual Shield 710",             "Flux-Cored Wire"),
    "DS 7100 SR":       ("Dual Shield 7100 SR",         "Flux-Cored Wire"),
    "DS T 75":          ("Dual Shield T-75",            "Flux-Cored Wire"),
    "DS R 70":          ("Dual Shield R 70",            "Flux-Cored Wire"),
    "CS 3":             ("Coreshield 3",                "Self-Shielded Flux-Cored Wire"),
    "CS 6":             ("Coreshield 6",                "Self-Shielded Flux-Cored Wire"),
    "ESAB 71":          ("ESAB 71",                     "Flux-Cored Wire (E71T-1)"),
    "WELD 71T 9":       ("Weld 71T-9",                  "Flux-Cored Wire (E71T-9)"),
    # More stick electrodes
    "AA 6010":          ("Atom Arc 6010",               "Stick Electrode (E6010)"),
    "AA 6011":          ("Atom Arc 6011",               "Stick Electrode (E6011)"),
    "AA 7014":          ("Atom Arc 7014",               "Stick Electrode (E7014)"),
    "AA 7024":          ("Atom Arc 7024",               "Stick Electrode (E7024)"),
    "SW 10":            ("Sureweld 10",                 "Stick Electrode (E6010)"),
    "SW 11":            ("Sureweld 11",                 "Stick Electrode (E6011)"),
    "SW 14":            ("Sureweld 14",                 "Stick Electrode (E7014)"),
    "SW 18":            ("Sureweld 18",                 "Stick Electrode (E7018)"),
    "SW 24":            ("Sureweld 24",                 "Stick Electrode (E7024)"),
    # Additional MIG/submerged arc
    "65 E":             ("Spoolarc 65 E",               "Submerged Arc Wire"),
    "AS 70S":           ("AutoShield 70S",              "Submerged Arc Wire (ER70S)"),
    "HELIX E WB":       ("Helix E WB",                  "Submerged Arc Wire"),
    # Hardfacing
    "STOODY":           ("Stoody",                      "Hardfacing Wire/Rod"),
    "THERMACLAD":       ("ThermaClad",                  "Hardfacing Wire"),
    # ESAB gun/torch product lines
    "SPRAY MASTER":     ("SprayMaster",                 "MIG Gun"),
    "SPRAYMASTER":      ("SprayMaster",                 "MIG Gun"),
    "TWECO ELITE":      ("Tweco Elite",                 "MIG Gun"),
    "CLASSIC":          ("Classic",                     "MIG Gun"),
    "WELDSKILL":        ("WeldSkill",                   "MIG Gun"),
    "SUPRAXT":          ("SupraXT",                     "MIG Gun"),
    "COMPACT ELIMINATOR": ("Compact Eliminator",        "MIG Gun"),
    "COMPACTELIMINATOR":  ("Compact Eliminator",        "MIG Gun"),
    "ESAB HELIARC SR":  ("ESAB Heliarc SR",             "TIG Torch"),
    "SL100":            ("SL100",                       "Plasma Torch"),
    "SL60":             ("SL60",                        "Plasma Torch"),
    # TBI automated torches
    "TBI 511 AUT":      ("TBI 511",                     "Automated MIG Torch"),
    "TBI 360 AUT":      ("TBI 360",                     "Automated MIG Torch"),
    "TBI 6G AUT":       ("TBI 6G",                      "Automated MIG Torch"),
    "TBI 7G AUT":       ("TBI 7G",                      "Automated MIG Torch"),
    "TBI 8G AUT":       ("TBI 8G",                      "Automated MIG Torch"),
    "TBI":              ("TBI",                         "MIG Torch"),
    # Welding machines
    "REBEL EMP 285IC":  ("Rebel EMP 285ic",             "Multi-Process Welder"),
    "REBEL EMP 235IC":  ("Rebel EMP 235ic",             "Multi-Process Welder"),
    "REBEL EMP 215IC":  ("Rebel EMP 215ic",             "Multi-Process Welder"),
    "REBEL EMP 205IC":  ("Rebel EMP 205ic",             "Multi-Process Welder"),
    "REBEL EMP":        ("Rebel EMP",                   "Multi-Process Welder"),
    "REBEL":            ("Rebel",                       "Welding Machine"),
    # Wire feeders
    "ROBUST FEED PRO":  ("Robust Feed PRO",             "Wire Feeder"),
    "ROBUST FEED PULSE":("Robust Feed Pulse",           "Wire Feeder"),
    "ROBUST FEED U82":  ("Robust Feed U82",             "Wire Feeder"),
    "ROBUST FEED U6":   ("Robust Feed U6",              "Wire Feeder"),
    "ROBUST FEED U0":   ("Robust Feed U0",              "Wire Feeder"),
    "ROBUST FEED":      ("Robust Feed",                 "Wire Feeder"),
    # Regulators — ETS Edge Two-Stage
    "ETS42 200":        ("ETS42-200",                   "Edge Two-Stage Regulator"),
    "ETS4":             ("ETS4",                        "Edge Two-Stage Regulator"),
    "ETS":              ("ETS",                         "Edge Regulator"),
    # Airco-style tips
    "AIRCO 138":        ("Airco 138",                   "Airco-Style Cutting Tip"),
    "AIRCO 144":        ("Airco 144",                   "Airco-Style Cutting Tip"),
    "AIRCO 164":        ("Airco 164",                   "Airco-Style Welding Tip"),
    "AIRCO":            ("Airco",                       "Airco-Style Gas Tip"),
    # TIG torches — SR series (ESAB Heliarc / WeldCraft)
    "SR 26 RMT":        ("SR-26 RMT",                   "TIG Torch (200A Gas Cooled)"),
    "SR 26V FX":        ("SR-26V FX",                   "TIG Torch (200A Gas Cooled)"),
    "SR 26FV":          ("SR-26FV",                     "TIG Torch (200A Gas Cooled)"),
    "SR 26":            ("SR-26",                       "TIG Torch (200A Gas Cooled)"),
    "SR 21 FX":         ("SR-21 FX",                    "TIG Torch (200A Gas Cooled)"),
    "SR 18 RMT":        ("SR-18 RMT",                   "TIG Torch (350A Water Cooled)"),
    "SR 20 FX":         ("SR-20 FX",                    "TIG Torch (250A Water Cooled)"),
    "SR 9 RMT":         ("SR-9 RMT",                    "TIG Torch (125A Gas Cooled)"),
    "SR 17 RMT":        ("SR-17 RMT",                   "TIG Torch (150A Gas Cooled)"),
    "SR":               ("SR Series",                   "TIG Torch"),
    # HW = Heliarc/WeldCraft legacy TIG torch bodies
    "HW 26FV":          ("WeldCraft 26FV",              "TIG Torch Body (200A, Flex w/Valve)"),
    "HW 17FV":          ("WeldCraft 17FV",              "TIG Torch Body (150A, Flex w/Valve)"),
    "HW 17RV":          ("WeldCraft 17RV",              "TIG Torch Body (150A, Rigid w/Valve)"),
    "HW 20":            ("WeldCraft 20",                "TIG Torch Body (250A Water Cooled)"),
    "HW 9":             ("WeldCraft 9",                 "TIG Torch Body (125A Gas Cooled)"),
    "HW 90180":         ("WeldCraft 90/180",            "TIG Torch Body"),
    "HW":               ("WeldCraft",                   "TIG Torch Body/Part"),
    # PT = Plasma torch heads/bodies
    "PT 27":            ("PT-27",                       "Plasma Torch"),
    "PT 25":            ("PT-25",                       "Plasma Torch"),
    "PT 31":            ("PT-31",                       "Plasma Torch"),
    "PT 36":            ("PT-36",                       "Plasma Torch"),
    "PT 38":            ("PT-38",                       "Plasma Torch"),
    "PT 121":           ("PT-121",                      "Plasma Torch"),
    "PT 26P":           ("PT-26P",                      "Plasma Torch"),
    # Cutmaster plasma systems
    "CUTMASTER 120":    ("Cutmaster 120",               "Plasma Cutting System"),
    "CUTMASTER 82":     ("Cutmaster 82",                "Plasma Cutting System"),
    "CUTMASTER 60":     ("Cutmaster 60",                "Plasma Cutting System"),
    "CUTMASTER 40":     ("Cutmaster 40",                "Plasma Cutting System"),
    "CUTMASTER A120":   ("Cutmaster A120",              "Plasma Cutting System"),
    "CUTMASTER A80":    ("Cutmaster A80",               "Plasma Cutting System"),
    "CUTMASTER A40":    ("Cutmaster A40",               "Plasma Cutting System"),
    "CUTMASTER":        ("Cutmaster",                   "Plasma Cutting System"),
    # Purox/Oxweld gas tips (Victor/ESAB)
    "PUROX STYLE 4202": ("Purox Style 4202",            "Oxy-Fuel Cutting Tip"),
    "PUROX 4202":       ("Purox 4202",                  "Oxy-Fuel Cutting Tip"),
    "PUROX STYLE":      ("Purox Style",                 "Oxy-Fuel Tip"),
    "PUROX":            ("Purox",                       "Oxy-Fuel Tip"),
    "OXWELD 1502":      ("Oxweld 1502",                 "Oxy-Fuel Cutting Tip"),
    "OXWELD 150212":    ("Oxweld 1502-12",              "Oxy-Fuel Cutting Tip"),
    "OXWELD":           ("Oxweld",                      "Oxy-Fuel Tip"),
    # ARC = Arcaloy metal-cored wire (MC prefix in description)
    "ARC MC409TI":      ("Arcaloy MC409Ti",             "Stainless Metal-Cored Wire"),
    "ARC MC439TI":      ("Arcaloy MC439Ti",             "Stainless Metal-Cored Wire"),
    "ARC MC18CRCB":     ("Arcaloy MC18CrCb",            "Stainless Metal-Cored Wire"),
    "ARC MC":           ("Arcaloy Metal-Cored",         "Stainless Metal-Cored Wire"),
    # Additional Sureweld stainless stick
    "SUREWELD 308L":    ("Sureweld 308L-16",            "Stainless Stick Electrode (E308L-16)"),
    "SUREWELD 309L":    ("Sureweld 309L-16",            "Stainless Stick Electrode (E309L-16)"),
    "SUREWELD 316L":    ("Sureweld 316L-16",            "Stainless Stick Electrode (E316L-16)"),
    # Sentinel welding helmets
    "SENTINEL A70PRO":  ("Sentinel A70 PRO",            "Auto-Darkening Welding Helmet"),
    "SENTINEL A60":     ("Sentinel A60",                "Auto-Darkening Welding Helmet"),
    "SENTINEL A50":     ("Sentinel A50",                "Auto-Darkening Welding Helmet"),
    "SENTINEL":         ("Sentinel",                    "Welding Helmet"),
    # ESAB Autrod (European wire, number prefix)
    "ESAB AUTROD 12":   ("ESAB Autrod 12",              "MIG Wire (ESAB Europe)"),
    "ESAB AUTROD":      ("ESAB Autrod",                 "MIG/TIG Wire (ESAB Europe)"),
    "ESAB C6M":         ("ESAB CoreWeld C6M",           "Metal-Cored Wire"),
    # Aluminum alloys — bare number (not ER-prefix, not R-prefix)
    "1100 O":           ("Alcotec 1100-O",              "Aluminum Wire (1100)"),
    "1100":             ("Alcotec 1100",                "Aluminum Wire (1100)"),
    "1350 F":           ("Alcotec 1350-F",              "Aluminum Wire (1350)"),
    "1350":             ("Alcotec 1350",                "Aluminum Wire (1350)"),
    "4008 F":           ("Alcotec 4008-F",              "Aluminum Wire (4008)"),
    "4008":             ("Alcotec 4008",                "Aluminum Wire (4008)"),
    "4043A":            ("Alcotec 4043A",               "Aluminum Wire (4043A)"),
    "4047 O":           ("Alcotec 4047-O",              "Aluminum Wire (4047)"),
    "5183":             ("Alcotec 5183",                "Aluminum Wire (5183)"),
    "5754":             ("Alcotec 5754",                "Aluminum Wire (5754)"),
    "7050":             ("Alcotec 7050",                "Aluminum Wire (7050)"),
    # TAM = Tweco Automatic/Manual MIG gun connections
    "TAM SERIES AUTO":  ("Tweco TAM Series",            "Automatic MIG Gun Connection"),
    "TAM BODY":         ("Tweco TAM Body",              "MIG Gun Connection Body"),
    "TAM":              ("Tweco TAM",                   "MIG Gun Connection Part"),
    # Sandvik stainless stick electrodes
    "SANDVIK E317L":    ("Sandvik E317L-16",            "Stainless Stick Electrode (E317L-16)"),
    "SANDVIK E309L":    ("Sandvik E309L-16",            "Stainless Stick Electrode (E309L-16)"),
    "SANDVIK E316L":    ("Sandvik E316L-16",            "Stainless Stick Electrode (E316L-16)"),
    "SANDVIK E308L":    ("Sandvik E308L-16",            "Stainless Stick Electrode (E308L-16)"),
    "SANDVIK":          ("Sandvik",                     "Stainless Stick Electrode"),
    # Additional ESAB products
    "ESAB TEACH TOOL":  ("ESAB Teach Tool",             "Cobot Welding Teaching Pendant"),
    "ESAB RC REMOTE":   ("ESAB RC Remote Control",      "Remote Control for Welding"),
    "ESAB STANDARD":    ("ESAB Standard Interface",     "Welding System Interface"),
    "ESAB 7018":        ("ESAB 7018",                   "Stick Electrode (E7018)"),
    "ESAB RUFFIAN":     ("ESAB Ruffian",                "Engine-Driven Welder"),
    "ESAB":             ("ESAB",                        "Welding Product"),
    # Flux-related
    "FLUX CORE SEFC":   ("Flux Core SEFC",              "Flux Core Seam Track Liner"),
    "FLUX HOPPER":      ("Flux Hopper",                 "Submerged Arc Flux Hopper"),
}

# Diameter patterns: code → human readable
DIAMETER_MAP = {
    "023": '0.023"', "024": '0.024"', "030": '0.030"', "035": '0.035"',
    "040": '0.040"', "045": '0.045"', "047": '3/64"', "052": '0.052"',
    "062": '0.062"', "068": '0.068"', "078": '5/64"', "093": '3/32"',
}

FRACTION_DIAMETERS = {
    "1 16": '1/16"', "5 64": '5/64"', "3 32": '3/32"', "7 64": '7/64"',
    "1 8": '1/8"', "9 64": '9/64"', "5 32": '5/32"', "3 16": '3/16"',
    "7 32": '7/32"', "1 4": '1/4"', "5 16": '5/16"',
}

METRIC_DIAMETERS = {
    "0 6MM": "0.6mm", "0 8MM": "0.8mm", "0 9MM": "0.9mm",
    "1 0MM": "1.0mm", "1 2MM": "1.2mm", "1 4MM": "1.4mm", "1 6MM": "1.6mm",
    "2 0MM": "2.0mm", "2 4MM": "2.4mm", "3 2MM": "3.2mm",
}

# Package code patterns → human readable
PACKAGE_PATTERNS = [
    # Drums and large packages
    (r'(\d+)F\s*DR(?:\s|$)', lambda m: f'{m.group(1)} lb Drum'),
    (r'(\d+)F\s*MP(?:\s|$)', lambda m: f'{m.group(1)} lb Pack'),
    (r'(\d+)F\s*OMP(?:\s|$)', lambda m: f'{m.group(1)} lb Pack'),
    # Spools
    (r'(\d+)F\s*PSP(?:\s|$)', lambda m: f'{m.group(1)} lb Plastic Spool'),
    (r'(\d+)F\s*FSP(?:\s|$)', lambda m: f'{m.group(1)} lb Fiber Spool'),
    (r'(\d+)F\s*SP(?:\s|$)', lambda m: f'{m.group(1)} lb Spool'),
    (r'(\d+)F\s*AWS(?:\s|$)', lambda m: f'{m.group(1)} lb Spool'),
    (r'(\d+)F\s*WB(?:\s|$)', lambda m: f'{m.group(1)} lb Wire Basket'),
    # Coils
    (r'(\d+)F\s*CL(?:\s|$)', lambda m: f'{m.group(1)} lb Coil'),
    # Stick electrode lengths
    (r'(\d+)X(\d+)F\s*HS(?:\s|$)', lambda m: f'{m.group(1)}" x {m.group(2)} lb Hermetically Sealed'),
    (r'(\d+)X(\d+)F\s*CT(?:\s|$)', lambda m: f'{m.group(1)}" x {m.group(2)} lb Carton'),
    (r'(\d+)X(\d+)FT', lambda m: f'{m.group(1)}" x {m.group(2)} lb Tube'),
    (r'(\d+)X(\d+)F\s*VACPAK', lambda m: f'{m.group(1)}" x {m.group(2)} lb Vacuum Pack'),
    # Kilogram packs
    (r'(\d+)KG\s*CP', lambda m: f'{m.group(1)} kg Pack'),
    (r'(\d+)KG', lambda m: f'{m.group(1)} kg'),
    # Bare weight
    (r'(\d+)F(?:\s+\d+F?\s*(?:PLT|CT))?$', lambda m: f'{m.group(1)} lb'),
    # Small spools
    (r'X\s*1F\b', lambda _: '1 lb Spool'),
]

# Hardgoods category keywords → (category, expanded_name)
# Order matters: more specific entries first to avoid false matches
HARDGOODS_CATEGORIES = [
    # Plasma consumables (must be before generic ELECTRODE/TIP)
    (["TIP TIP GOUGING"], "Plasma Gouging Tip"),
    (["TIP TIP LONG"], "Plasma Cutting Tip (Long)"),
    (["TIP TIP EXTENDED"], "Plasma Cutting Tip (Extended)"),
    (["TIP TIP"], "Plasma Cutting Tip"),
    (["ELECTRODE ELECTRODE AIR", "ELECTRODE AIR"], "Air Carbon Arc Electrode"),
    (["ELECTRODE ELECTRODE O2", "ELECTRODE ELECTRODE NITROGEN",
     "ELECTRODE ELECTRODE AR", "ELECTRODE ELECTRODE MULTI",
     "ELECTRODE ELECTRODE GOUGING", "ELECTRODE ELECTRODE MG",
     "ELECTRODE ELECTRODE N2", "ELECTRODE ELECTRODE WELDING",
     "ELECTRODE ELECTRODE LO AMP"], "Plasma Electrode"),
    (["ELECTRODE PT"], "Plasma Electrode"),
    # TIG torch parts
    (["GAS LENS"], "TIG Gas Lens"),
    (["CUP HI", "CUP CER", "SHIELD CUP"], "TIG Cup"),
    (["COLLET BODY"], "TIG Collet Body"),
    (["COLLET"], "TIG Collet"),
    (["BACK CAP"], "TIG Back Cap"),
    (["TIG TORCH"], "TIG Torch"),
    # MIG consumables
    (["CONTACT TIP"], "Contact Tip"),
    (["CONTACT JAW"], "Contact Jaw"),
    (["CONTACT TUBE"], "Contact Tube"),
    (["CT HOLDER"], "Contact Tip Holder"),
    (["NOZZLE"], "Nozzle"),
    (["GAS DIFFUSER"], "Gas Diffuser"),
    (["GAS DISTRIBUTOR"], "Gas Distributor"),
    (["SHIELD CAP"], "Shield Cap"),
    (["SHIELD RETAINER"], "Shield Retaining Cap"),
    (["NOZ RETAIN"], "Nozzle Retaining Cup"),
    (["DRIVE RL", "DRIVE ROLL"], "Drive Roll"),
    (["FEEDROLL", "FEED ROLL", "FEED ROLLER"], "Feed Roll"),
    (["CONDUCTOR TUBE"], "Conductor Tube"),
    (["LINER"], "MIG Liner"),
    (["GUIDE TUBE"], "Wire Guide Tube"),
    (["WIRE GUIDE"], "Wire Guide"),
    (["WIRE OUTLET", "OUTLET GUIDE"], "Wire Outlet Guide"),
    (["INLET GUIDE"], "Wire Inlet Guide"),
    (["CENTER GUIDE"], "Center Wire Guide"),
    (["INSULATOR"], "Insulator"),
    # Torch / gun types
    (["TORCH BODY", "TORCH ADAPTER"], "Torch"),
    (["TORCH HEAD"], "Torch Head"),
    (["TORCH HOLDER"], "Torch Holder"),
    (["TORCH CARRIAGE"], "Torch Carriage"),
    (["TORCH LEADS"], "Torch Leads Package"),
    (["SPRAYMASTER", "SPRAY MASTER"], "SprayMaster MIG Gun"),
    (["TWECO ELITE"], "Tweco Elite MIG Gun"),
    (["TWECO"], "Tweco MIG Gun Part"),
    (["HELIARC"], "ESAB Heliarc TIG Torch"),
    (["EXEOR MIG"], "Exeor MIG Gun"),
    (["QRW ROBOTIC", "QRA ROBOTIC", "QRWA ROBOTIC"], "Robotic MIG Gun"),
    (["PUSHPULL", "PUSH PULL"], "Push-Pull MIG Gun"),
    (["SWAN NECK"], "Robotic Swan Neck"),
    (["WATER COOLED MIG"], "Water-Cooled MIG Gun"),
    # Cutting / gas apparatus
    (["CUTTING TIP", "TIP TIP AIR"], "Cutting Tip"),
    (["VICTOR STYLE"], "Victor-Style Gas Apparatus"),
    (["SMITH STYLE"], "Smith-Style Gas Apparatus"),
    (["HARRIS STYLE"], "Harris-Style Gas Apparatus"),
    (["AIRCO STYLE"], "Airco-Style Gas Apparatus"),
    (["SEAT STEM"], "Seat Stem Assembly"),
    (["SWIVEL BRASS"], "Brass Swivel Fitting"),
    (["INNER OXYGEN"], "Inner Oxygen Tube"),
    (["MACHINE CUTTING TORCH"], "Machine Cutting Torch"),
    # Welding electrode types
    (["TUNG ELEC", "TUNGSTEN"], "Tungsten Electrode"),
    (["ELECTRODE HOLDER"], "Electrode Holder"),
    # Power equipment
    (["POWER SUPPLY", "POWER SOURCE"], "Power Supply"),
    (["WIRE FEEDER", "WIREFEED"], "Wire Feeder"),
    (["WARRIOR"], "Warrior Welding Machine"),
    (["ARISTO"], "Aristo Welding Machine"),
    (["ROGUE"], "Rogue Welding Machine"),
    (["COOL MINI"], "Cool Mini Water Cooler"),
    # Regulators / gas
    (["REGULATOR"], "Regulator"),
    (["RELIEF VALVE"], "Relief Valve"),
    (["SOLENOID VALVE"], "Solenoid Valve"),
    (["PRESSURE ROLL", "PRESSURE ROLLER"], "Pressure Roll"),
    (["PRESSURE DEVICE"], "Pressure Device"),
    (["PRESSURE CONTROL"], "Pressure Control"),
    # Cables / connectors / hoses
    (["CABLE"], "Cable"),
    (["WELDING CABLE"], "Welding Cable"),
    (["EDGE INTERC"], "Edge Interconnection Cable"),
    (["CONN SET"], "Connector Set"),
    (["CONNECTOR"], "Connector"),
    (["QUICK CONNECTOR"], "Quick Connector"),
    (["HOSE ASSY", "HOSE CONN", "HOSE UNION", "HOSE PACKAGE"], "Hose Assembly"),
    (["FLEX CONDTUBE", "CONDUIT"], "Conduit / Liner"),
    # Hardware / fasteners / misc parts
    (["HANDLE"], "Handle"),
    (["KNOB"], "Knob"),
    (["SWITCH"], "Switch"),
    (["NUT BRASS"], "Brass Nut"),
    (["NUT"], "Nut"),
    (["SCREW"], "Screw"),
    (["WASHER"], "Washer"),
    (["O RING"], "O-Ring"),
    (["SPRING"], "Spring"),
    (["COVER"], "Cover"),
    (["SHIELD"], "Shield"),
    (["FRONT PANEL", "FRONT DECORATIVE"], "Front Panel"),
    (["FRONT COVER LENS"], "Front Cover Lens"),
    (["FRONT WHEEL"], "Front Wheel Kit"),
    (["FRONT"], "Front Part"),
    (["HEAT SHIELD"], "Heat Shield"),
    # Electrical / electronics
    (["PCB CONTROL", "PCB"], "Circuit Board (PCB)"),
    (["PC BOARD", "PFC BOARD"], "Circuit Board"),
    (["CIRCUIT BREAKER"], "Circuit Breaker"),
    (["MOTOR"], "Motor"),
    (["FAN"], "Fan"),
    (["FILTER"], "Filter"),
    (["PANEL"], "Panel"),
    # Machine accessories
    (["REAMER"], "Nozzle Reamer"),
    (["MIXER"], "Torch Mixer"),
    (["GAUGE"], "Pressure Gauge"),
    (["VALVE BODY", "VALVE STEM", "VALVE ASSY"], "Valve Assembly"),
    (["DIAPHRAGM"], "Diaphragm"),
    (["MAGNETIC EARTH CLAMP"], "Magnetic Earth Clamp"),
    (["MAGNETIC TORCH HOLDER"], "Magnetic Torch Holder"),
    (["COLLISION PROTECTION", "COLLISION POTECTION"], "Collision Protection"),
    (["LASER PROTECTION"], "Laser Protection Cover"),
    (["SPARE PARTS KIT"], "Spare Parts Kit"),
    (["REPAIR KIT"], "Repair Kit"),
    # Branded kits/packages
    (["PACK CUTSKILL"], "Cutskill Plasma Consumable"),
    (["PLASMA"], "Plasma Torch Part"),
    (["LABEL"], "Label"),
    (["STOODY"], "Stoody Hardfacing"),
    (["THERMACLAD"], "ThermaClad Hardfacing"),
    (["CTD", "VACPAK"], "TIG Rod / Cut Length"),
    # Wire/guide misc
    (["WIRE STRAIGHTENER"], "Wire Straightener"),
    (["WIRE REEL"], "Wire Reel"),
    (["CONTROL WIRE"], "Control Wire"),
    # Guide catch-all
    (["GUIDE PIN"], "Guide Pin"),
    (["GUIDE WHEEL"], "Guide Wheel"),
    (["GUIDE FINGER"], "Guide Finger"),
    # Gas apparatus / fittings
    (["GAS SEPARATOR"], "Gas Separator"),
    (["GAS HOSE"], "Gas Hose"),
    (["GAS COOLED"], "Gas-Cooled Torch Part"),
    (["GAS SYSTEM"], "Gas System Component"),
    # Torch parts
    (["TORCH PB", "TORCH PUSH"], "Push-Pull MIG Torch"),
    (["TORCH NECK"], "Torch Neck"),
    (["TORCH BODY"], "Torch Body"),
    (["TORCH ADAPTER"], "Torch Adapter"),
    # Valve parts
    (["VALVE CONNECTION"], "Valve Connection"),
    (["VALVE CAP"], "Valve Cap"),
    (["VALVE"], "Valve"),
    # Machine parts
    (["SPACER"], "Spacer"),
    (["TRIGGER ASSY", "TRIGGER ASSEMBLY"], "Trigger Assembly"),
    (["TRIGGER LEVER"], "Trigger Lever"),
    (["TRIGGER"], "Trigger"),
    (["HARNESS SIGNAL"], "Signal Harness"),
    (["HARNESS SOLENOID"], "Solenoid Harness"),
    (["HARNESS GAS"], "Gas System Harness"),
    (["HARNESS"], "Wiring Harness"),
    (["SLEEVE PLUG"], "Sleeve Plug"),
    (["SLEEVE"], "Sleeve"),
    (["HEAD REPAIR"], "Torch Head Repair Part"),
    (["ARC CONTROL"], "Arc Control Box"),
    (["REAR AXLE"], "Rear Axle"),
    (["REAR CASE"], "Rear Case"),
    (["REAR"], "Rear Part"),
    (["BASE PLATE"], "Base Plate"),
    (["FLUX CORE SEFC"], "Flux Core Seam Track Liner"),
    (["FLUX HOPPER"], "Submerged Arc Flux Hopper"),
    (["CLEAR PLASTIC"], "Clear Plastic Cover"),
    (["WASTE BAG"], "Waste Bags"),
    (["ACETYLENE TUBE"], "Acetylene Tube"),
    (["OXYGEN TUBE"], "Oxygen Tube"),
    (["ACETYLENE HOSE"], "Acetylene Hose"),
    # PSF = Binzel-style torch parts
    (["PSF 250", "PSF 305", "PSF 410", "PSF 510"], "PSF MIG Torch Part"),
    (["PSF"], "PSF MIG Torch Part"),
    # Victor cutting/heating tips (numeric prefixes)
    (["MFTN HEATING", "MFTA HEATING"], "Victor Heating Tip"),
    (["HDN CUT"], "Victor Heavy Duty Cutting Tip"),
    (["GPP GUTTING", "GPN CUTTING"], "Victor Cutting Tip"),
    (["GTB GOUGING"], "Victor Gouging Tip"),
    # Slice plasma consumables
    (["SLICE TRH"], "Slice Plasma Torch Head"),
    (["SLICE"], "Slice Plasma Consumable"),
    # Robotic welding
    (["FOOT SENSOR"], "Foot Sensor"),
    (["MOUNTING BRACKET"], "Mounting Bracket"),
    (["PRESSURE BOGEY"], "Pressure Bogey Kit"),
    (["CASTING SHEAVE"], "Sheave Assembly"),
    (["RUBBER HOSE"], "Rubber Hose"),
    (["WELDING CURRENT"], "Welding Current Terminal"),
    (["CTO CTF"], "Cutting Torch Outfit"),
    (["LEADS PACK"], "Torch Leads Package"),
]

# These are checked differently — only match if description STARTS WITH the keyword
# to avoid false positives on generic words appearing anywhere
_STARTSWITH_CATEGORIES = [
    ("HOSE ", "Hose"),
    ("FITTING ", "Fitting"),
    ("ELECTRODE ", "Electrode Part"),
    ("TIP ", "Tip"),
    ("WIRE ", "Wire Part"),
    ("GUIDE ", "Guide"),
    ("SHIELD ", "Shield"),
    ("ADAPT ", "Adapter"),
    ("SENSOR ", "Sensor"),
    ("CLAMP ", "Clamp"),
    ("PLUG ", "Plug"),
    ("BRACKET ", "Bracket"),
    ("BEARING ", "Bearing"),
    ("BUSHING ", "Bushing"),
    ("SEAL ", "Seal"),
    ("GASKET ", "Gasket"),
    ("CABLE ASSY", "Cable Assembly"),
    ("CABLE ", "Cable"),
    ("BOARD ", "Board"),
    ("GEAR ", "Gear"),
    ("BELT ", "Belt"),
    ("SPINDLE ", "Spindle"),
    ("CAPACITOR ", "Capacitor"),
    ("CAP ", "Cap"),
    ("RELAY ", "Relay"),
    ("TRANSFORMER ", "Transformer"),
    ("CONTACTOR ", "Contactor"),
    ("RESISTOR ", "Resistor"),
    ("FUSE ", "Fuse"),
    ("PUMP ", "Pump"),
    ("WHEEL ", "Wheel"),
    ("PIN ", "Pin"),
    ("NIPPLE ", "Nipple"),
    ("ADAPTOR ", "Adaptor"),
    ("ADAPTER ", "Adapter"),
    ("BOOT ", "Boot"),
    ("END CAP", "End Cap"),
    ("DECAL ", "Decal"),
    ("STICKER ", "Label"),
    ("TORCH ", "Torch Part"),
    ("VALVE ", "Valve"),
    ("BODY ", "Body Part"),
    ("TUBE ", "Tube"),
    ("HEAD ", "Head"),
    ("SEAT ", "Seat"),
    ("SPACER ", "Spacer"),
    ("KIT ", "Kit"),
    ("ASSY ", "Assembly"),
    ("SCR ", "Screw/Fastener"),
    ("SLEEVE ", "Sleeve"),
    ("RUBBER ", "Rubber Part"),
    ("WELDING ", "Welding Part"),
    ("MOUNTING ", "Mounting Part"),
    ("FOOT ", "Foot Part"),
    ("PRESSURE ", "Pressure Part"),
    ("CASTING ", "Casting"),
    ("ROTOR ", "Rotor"),
    ("FERRULE ", "Ferrule"),
    ("CONE ", "Cone"),
    ("RACK ", "Rack/Rail"),
    ("STRIP ", "Strip Cutter"),
    ("ROLLER ", "Roller"),
    ("MESH ", "Mesh Screen"),
    ("CENTRALIZER", "Centralizer"),
    ("LEADS ", "Leads Package"),
    ("SLICE ", "Slice Plasma Part"),
]


def _match_product_line(desc: str) -> tuple[str, str] | None:
    """Try to match description against known product lines. Returns (full_name, type) or None."""
    desc_upper = desc.upper().strip()
    # Try longest match first
    for code in sorted(PRODUCT_LINES.keys(), key=len, reverse=True):
        if desc_upper.startswith(code + " ") or desc_upper == code:
            return PRODUCT_LINES[code]
    return None


def _extract_diameter(desc: str) -> str | None:
    """Extract diameter from description."""
    d = desc.upper()
    # 3-digit decimal (045, 035, etc.) — find ALL matches and use the first valid one
    for m in re.finditer(r'(\d{3})\s*X', d):
        if m.group(1) in DIAMETER_MAP:
            return DIAMETER_MAP[m.group(1)]
    # Fraction diameter (1 8, 3 32, 5 32, 1 16, etc.) followed by X
    for code, readable in FRACTION_DIAMETERS.items():
        if re.search(code.replace(' ', r'\s+') + r'\s*X', d):
            return readable
    # Fraction diameter without X (may appear anywhere)
    for code, readable in FRACTION_DIAMETERS.items():
        if re.search(r'\b' + code.replace(' ', r'\s+') + r'\b', d):
            return readable
    # Metric
    for code, readable in METRIC_DIAMETERS.items():
        if code.replace(' ', r'\s*') in d.replace(' ', ''):
            return readable
    return None


def _extract_package(desc: str) -> str | None:
    """Extract package/size from description."""
    d = desc.upper()
    # Try the X-delimited package patterns (e.g., 045X44FSP, 1 8X14X50FHS)
    # Get everything after the first NNNx or fraction-X
    xpart = re.search(r'\d{3}X(.+)', d)
    if not xpart:
        xpart = re.search(r'\d+\s+\d+X(.+)', d)
    if xpart:
        suffix = xpart.group(1)
        for pattern, formatter in PACKAGE_PATTERNS:
            m = re.search(pattern, suffix)
            if m:
                return formatter(m)
    return None


def _categorize_hardgood(desc: str) -> str | None:
    """Categorize a hardgood item."""
    d = desc.upper()
    # Check keyword-anywhere matches first (specific categories)
    for keywords, category in HARDGOODS_CATEGORIES:
        if any(kw in d for kw in keywords):
            return category
    # Check starts-with matches (generic catch-all categories)
    for prefix, category in _STARTSWITH_CATEGORIES:
        if d.startswith(prefix):
            return category
    return None


def decode_description(desc: str) -> str:
    """
    Decode an ESAB abbreviated description into human-readable text.

    Examples:
        "WELD 70S 6 045X44F" → "Spoolarc Weld 70S-6 | MIG Wire (ER70S-6) | 0.045\" x 44 lb"
        "AA 7018 1 8X14X50FHS" → "Atom Arc 7018 | Stick Electrode (E7018) | 1/8\" x 14\" x 50 lb Hermetically Sealed"
        "CONTACT TIP HD 035" → "Contact Tip | HD 035"
    """
    desc = str(desc).strip()
    if not desc:
        return ""

    parts = []

    # Try product line match
    line_match = _match_product_line(desc)
    if line_match:
        full_name, prod_type = line_match
        desc_upper = desc.upper()

        # Guard: short product codes (2-3 chars like DS, CS, SA, AS, AA, SW, CW)
        # can false-match hardgoods parts. If description has no diameter pattern
        # AND no X-delimited size AND the remainder looks like a hardgoods name,
        # fall through to hardgoods categorization instead.
        matched_code = None
        for code in sorted(PRODUCT_LINES.keys(), key=len, reverse=True):
            if desc_upper.startswith(code + " ") or desc_upper == code:
                matched_code = code
                break
        if matched_code and len(matched_code) <= 3:
            remainder = desc_upper[len(matched_code):].strip()
            has_size_pattern = bool(re.search(r'\d{3}\s*X|\d+\s+\d+\s*X', remainder))
            if not has_size_pattern:
                # Check if remainder looks like a hardgoods part
                hardgood = _categorize_hardgood(desc)
                if hardgood:
                    return f"{hardgood} | {desc}"

        # 36" cut-length items are TIG rods, not MIG wire / SAW wire
        # Pattern: X36 in description means 36-inch straight rod for TIG welding
        if "X36" in desc_upper and "TIG" not in prod_type.upper():
            # Override type to TIG Rod, preserve alloy info from original type
            alloy_info = ""
            paren = prod_type.find("(")
            if paren >= 0:
                alloy_info = " " + prod_type[paren:]
            prod_type = f"TIG Rod{alloy_info}"

        parts.append(full_name)
        parts.append(prod_type)

        diameter = _extract_diameter(desc)
        if diameter:
            parts.append(diameter)

        package = _extract_package(desc)
        if package:
            parts.append(package)

        return " | ".join(parts)

    # Try hardgoods
    category = _categorize_hardgood(desc)
    if category:
        return f"{category} | {desc}"

    # Regex fallback: items starting with 3-digit diameter code (e.g., "045 102 G 33F WB")
    # These are typically Stoody/hardfacing wires
    m = re.match(r'^(\d{3})\s+(.+)', desc.upper())
    if m and m.group(1) in DIAMETER_MAP:
        diam = DIAMETER_MAP[m.group(1)]
        remainder = m.group(2).strip()
        # Try to extract package from remainder
        pkg = _extract_package(desc)
        line_parts = [f"Hardfacing/Specialty Wire", diam]
        if pkg:
            line_parts.append(pkg)
        line_parts.append(remainder)
        return " | ".join(line_parts)

    # Regex fallback: items starting with fraction (e.g., "1 8 X 14 BARE BOROD 10F")
    m = re.match(r'^(\d+\s+\d+)\s+X\s+(.+)', desc.upper())
    if m:
        frac_code = m.group(1)
        if frac_code in FRACTION_DIAMETERS:
            diam = FRACTION_DIAMETERS[frac_code]
            remainder = m.group(2).strip()
            # Detect product type from keywords
            if any(kw in remainder for kw in ["BARE", "BOROD", "ATB", "CTS", "BTS"]):
                prod_type = "Bare Rod / Brazing Alloy"
            elif "HORSESHOE" in remainder:
                prod_type = "Horseshoe Bare Rod"
            else:
                prod_type = "Rod / Filler"
            return f"{prod_type} | {diam} | {remainder}"

    # Regex fallback: Victor gas apparatus parts with numeric prefix
    # e.g., "75 DEG HEAD", "315 BODY Y W VICTOR", "50 AMP XT TIP"
    d_upper = desc.upper()
    if re.match(r'^\d+\s+(DEG|DEGREE)\s+(HEAD|BODY)', d_upper):
        return f"Victor/Gas Apparatus | Torch Head | {desc}"
    if re.match(r'^\d+\s+(AMP|AMPS)\s+', d_upper):
        return f"Plasma Consumable | {desc}"
    if re.match(r'^\d+\s+(B\s+\d+\s+ELBOW|ELBOW)', d_upper):
        return f"Victor/Gas Apparatus | Elbow | {desc}"
    if re.match(r'^\d+\s+(MFTN|MFTA|MFT)\s+HEATING', d_upper):
        return f"Victor Heating Tip | {desc}"
    if re.match(r'^\d+\s+(HDN|HDA)\s+CUT', d_upper):
        return f"Victor Cutting Tip | {desc}"
    if re.match(r'^\d+\s+(GPP|GPN|GTB|GTS)\s+', d_upper):
        return f"Victor Gas Tip | {desc}"
    if re.match(r'^\d+\s+(LDS|LEADS)\s+', d_upper):
        return f"Torch Leads Package | {desc}"
    if re.match(r'^\d+\s+M\s+\d+', d_upper) or re.match(r'^\d+\s+4M\s+', d_upper):
        return f"MIG Gun | {desc}"

    # No match — return original
    return desc


# ── Web enrichment cache ───────────────────────────────────

_CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "descriptions.json"


def load_cache() -> dict[str, str]:
    """Load the web enrichment cache."""
    if _CACHE_PATH.exists():
        with open(_CACHE_PATH) as f:
            return json.load(f)
    return {}


def save_cache(cache: dict[str, str]):
    """Save the web enrichment cache."""
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def get_enriched_description(part_number: str, raw_description: str, cache: dict[str, str] | None = None) -> str:
    """
    Get the best available description for a product:
    1. Web cache (if available)
    2. Pattern decoder
    3. Raw description
    """
    pn = part_number.strip()

    # Check web cache first
    if cache and pn in cache:
        return cache[pn]

    # Pattern decode
    decoded = decode_description(raw_description)
    if decoded != raw_description:
        return decoded

    return raw_description


def enrich_dataframe(df: pd.DataFrame, cache: dict[str, str] | None = None) -> pd.Series:
    """Add enriched descriptions to a DataFrame. Returns a Series."""
    if cache is None:
        cache = load_cache()

    return pd.Series([
        get_enriched_description(str(row.get("part_number", "")), str(row.get("description", "")), cache)
        for _, row in df.iterrows()
    ], index=df.index)
